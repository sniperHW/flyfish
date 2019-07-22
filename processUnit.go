package flyfish

import (
	//"container/list"
	//"database/sql/driver"
	"flyfish/conf"
	"flyfish/errcode"
	"flyfish/proto"
	"fmt"
	"github.com/jmoiron/sqlx"
	"github.com/sniperHW/kendynet/util"
	//"net"
	"sync"
	"sync/atomic"
	"time"
)

/*
 *    每个processUnit负责处理其关联的key
 */

var processUnits []*processUnit

var sqlUpdateWg *sync.WaitGroup

type processUnit struct {
	cacheKeys   map[string]*cacheKey
	minheap     *util.MinHeap
	mtx         sync.Mutex
	tick        int64 //每次访问+1假设每秒访问100万次，需要运行584942年才会回绕
	sqlLoader_  *sqlLoader
	sqlUpdater_ *sqlUpdater
}

func (this *processUnit) updateQueueFull() bool {
	return this.sqlUpdater_.queue.Len() >= conf.DefConfig.SqlLoadEventQueueSize
}

func (this *processUnit) pushRedisReq(ctx *processContext, fullReturn ...bool) bool {
	err := redisProcessQueue.AddNoWait(ctx, fullReturn...)
	if nil == err {
		atomic.AddInt32(&redisReqCount, 1)
		return true
	} else {
		return false
	}
}

func (this *processUnit) pushSqlLoadReq(ctx *processContext, fullReturn ...bool) bool {
	ckey := ctx.getCacheKey()
	if ckey.isWriteBack() {
		/*
		 *   如果记录正在等待回写，redis崩溃，导致重新从数据库载入数据，
		 *   此时回写尚未完成，如果允许读取将可能载入过期数据
		 */
		ckey.clearCmd()
		return false

	} else {
		err := this.sqlLoader_.queue.AddNoWait(ctx, fullReturn...)
		if nil == err {
			return true
		} else {
			return false
		}
	}
}

/*
 *    insert + update = insert
 *    insert + delete = none
 *    insert + insert = 非法
 *    update + insert = 非法
 *    update + delete = delete
 *    update + update = update
 *    delete + insert = update
 *    delete + update = 非法
 *    delete + delte  = 非法
 *    none   + insert = insert
 *    none   + update = 非法
 *    node   + delete = 非法
 */

func (this *processUnit) pushSqlWriteBackReq(ctx *processContext) bool {

	if ctx.writeBackFlag == write_back_none {
		Errorln("ctx.writeBackFlag == write_back_none")
		return false
	}

	ckey := ctx.getCacheKey()
	ckey.mtx.Lock()
	ckey.writeBacked = true

	wb := ckey.r
	if nil == wb {
		wb = recordGet()
		wb.writeBackFlag = ctx.writeBackFlag
		wb.key = ctx.getKey()
		wb.table = ctx.getTable()
		wb.uniKey = ctx.getUniKey()
		wb.ckey = ckey
		if wb.writeBackFlag == write_back_insert || wb.writeBackFlag == write_back_update {
			wb.fields = map[string]*proto.Field{}
			for k, v := range ctx.fields {
				wb.fields[k] = v
			}
		}

		if ctx.replyOnDbOk {
			wb.ctx = ctx
		}
		ckey.r = wb

		ckey.mtx.Unlock()

		err := this.sqlUpdater_.queue.AddNoWait(ckey)
		if nil == err {
			return true
		} else {
			ckey.clearWriteBack()
			return false
		}

	} else {

		//合并状态
		if wb.writeBackFlag == write_back_insert {
			/*
			*    insert + update = insert
			*    insert + delete = none
			*    insert + insert = 非法
			 */
			if ctx.writeBackFlag == write_back_delete {
				wb.fields = nil
				wb.writeBackFlag = write_back_none
			} else {
				for k, v := range ctx.fields {
					wb.fields[k] = v
				}
			}
		} else if wb.writeBackFlag == write_back_update {
			/*
			 *    update + insert = 非法
			 *    update + delete = delete
			 *    update + update = update
			 */
			if ctx.writeBackFlag == write_back_insert {
				//逻辑错误，记录日志
			} else if ctx.writeBackFlag == write_back_delete {
				wb.fields = nil
				wb.writeBackFlag = write_back_delete
			} else {
				for k, v := range ctx.fields {
					wb.fields[k] = v
				}
			}
		} else if wb.writeBackFlag == write_back_delete {
			/*
			*    delete + insert = update
			*    delete + update = 非法
			*    delete + delte  = 非法
			 */
			if ctx.writeBackFlag == write_back_insert {
				wb.fields = map[string]*proto.Field{}
				for k, v := range ctx.fields {
					wb.fields[k] = v
				}
				meta := wb.ckey.meta.fieldMetas
				for k, v := range meta {
					if nil == wb.fields[k] {
						//使用默认值填充
						wb.fields[k] = proto.PackField(k, v.defaultV)
					}
				}
				wb.writeBackFlag = write_back_update
			} else {
				//逻辑错误，记录日志
			}
		} else {
			/*
			*    none   + insert = insert
			*    none   + update = 非法
			*    node   + delete = 非法
			 */
			if ctx.writeBackFlag == write_back_insert {
				wb.fields = map[string]*proto.Field{}
				for k, v := range ctx.fields {
					wb.fields[k] = v
				}
				meta := wb.ckey.meta.fieldMetas
				for k, v := range meta {
					if nil == wb.fields[k] {
						//使用默认值填充
						wb.fields[k] = proto.PackField(k, v.defaultV)
					}
				}
				wb.writeBackFlag = write_back_insert
			} else {
				//逻辑错误，记录日志
			}
		}

		if ctx.replyOnDbOk {
			wb.ctx = ctx
		}

		ckey.mtx.Unlock()
	}

	return true
}

func (this *cacheKey) process_(fromClient bool, cmd ...*command) {

	this.mtx.Lock()

	if len(cmd) > 0 {
		this.cmdQueue.PushBack(cmd[0])
	} else {
		this.unlock()
	}

	if this.locked || this.cmdQueue.Len() == 0 {
		this.mtx.Unlock()
		return
	}

	cmdQueue := this.cmdQueue
	e := cmdQueue.Front()

	if nil == e {
		this.mtx.Unlock()
		return
	}

	ctx := &processContext{
		commands: []*command{},
		fields:   map[string]*proto.Field{},
	}

	lastCmdType := cmdNone

	now := time.Now()

	for ; nil != e; e = cmdQueue.Front() {
		cmd := e.Value.(*command)
		if now.After(cmd.deadline) {
			//已经超时
			cmdQueue.Remove(e)
			atomic.AddInt32(&cmdCount, -1)
		} else {
			ok := false
			if cmd.cmdType == cmdGet {
				if !(lastCmdType == cmdNone || lastCmdType == cmdGet) {
					break
				}
				cmdQueue.Remove(e)
				ok = this.processGet(ctx, cmd)
				if ok {
					lastCmdType = cmd.cmdType
				}
			} else if lastCmdType == cmdNone {
				cmdQueue.Remove(e)
				switch cmd.cmdType {
				case cmdSet:
					ok = this.processSet(ctx, cmd)
					break
				case cmdSetNx:
					ok = this.processSetNx(ctx, cmd)
					break
				case cmdCompareAndSet:
					ok = this.processCompareAndSet(ctx, cmd)
					break
				case cmdCompareAndSetNx:
					ok = this.processCompareAndSetNx(ctx, cmd)
					break
				case cmdIncrBy:
					ok = this.processIncrBy(ctx, cmd)
					break
				case cmdDecrBy:
					ok = this.processDecrBy(ctx, cmd)
					break
				case cmdDel:
					ok = this.processDel(ctx, cmd)
					break
				default:
					//记录日志
					break
				}

				if ok {
					lastCmdType = cmd.cmdType
					break
				}

			} else {
				break
			}
		}
	}

	if lastCmdType == cmdNone {
		this.mtx.Unlock()
		return
	}

	if fromClient && causeWriteBackCmd(lastCmdType) {
		if this.unit.updateQueueFull() {
			this.mtx.Unlock()
			ctx.reply(errcode.ERR_BUSY, nil, -1)
			return
		}
	}

	fullReturn := false
	if fromClient {
		fullReturn = true
	}

	ok := true

	this.lock()
	this.mtx.Unlock()

	if this.status == cache_ok || this.status == cache_missing {
		ok = this.unit.pushRedisReq(ctx, fullReturn)
	} else {
		ok = this.unit.pushSqlLoadReq(ctx, fullReturn)
	}

	if !ok {
		this.mtx.Lock()
		this.unlock()
		this.mtx.Unlock()
		ctx.reply(errcode.ERR_BUSY, nil, -1)
	}
}

func getUnitByUnikey(uniKey string) *processUnit {
	return processUnits[StringHash(uniKey)%conf.DefConfig.CacheGroupSize]
}

func getCacheKey(table string, uniKey string) *cacheKey {
	unit := getUnitByUnikey(uniKey)
	defer unit.mtx.Unlock()
	unit.mtx.Lock()
	k, ok := unit.cacheKeys[uniKey]
	if ok {
		k.updateLRU()
	} else {
		k = newCacheKey(unit, table, uniKey)
		if nil != k {
			k.updateLRU()
			unit.cacheKeys[uniKey] = k
		}
	}
	return k
}

func (this *processUnit) kickCacheKey() {
	for !isStop() {
		time.Sleep(time.Second)
		this.mtx.Lock()
		for len(this.cacheKeys) > conf.DefConfig.MaxCachePerGroupSize {
			min := this.minheap.Min()
			if nil == min {
				break
			}
			c := min.(*cacheKey)
			var locked bool
			c.mtx.Lock()
			locked = c.locked
			c.mtx.Unlock()
			if locked {
				break
			}
			this.minheap.PopMin()
			delete(this.cacheKeys, c.uniKey)

			cmd := &command{
				uniKey: c.uniKey,
				ckey:   c,
			}

			ctx := &processContext{
				commands:  []*command{cmd},
				redisFlag: redis_kick,
			}

			this.pushRedisReq(ctx)
		}

		this.mtx.Unlock()
	}
}

func InitProcessUnit() bool {

	placeHolderInit()

	sqlUpdateWg = &sync.WaitGroup{}

	ping := func(q *util.BlockQueue) {
		for {
			//每60秒请求一次ping
			if util.ErrQueueClosed == q.AddNoWait(&processContext{ping: true}) {
				break
			}
			time.Sleep(time.Second * 60)
		}
	}

	processUnits = make([]*processUnit, conf.DefConfig.CacheGroupSize)
	for i := 0; i < conf.DefConfig.CacheGroupSize; i++ {

		lname := fmt.Sprintf("sqlLoad:%d", i)
		wname := fmt.Sprintf("sqlUpdater:%d", i)

		var loadDB *sqlx.DB
		var writeBackDB *sqlx.DB
		var err error
		dbConfig := conf.DefConfig.DBConfig

		loadDB, err = sqlOpen(dbConfig.SqlType, dbConfig.DbHost, dbConfig.DbPort, dbConfig.DbDataBase, dbConfig.DbUser, dbConfig.DbPassword)

		if nil != err {
			return false
		}

		writeBackDB, err = sqlOpen(dbConfig.SqlType, dbConfig.DbHost, dbConfig.DbPort, dbConfig.DbDataBase, dbConfig.DbUser, dbConfig.DbPassword)

		if nil != err {
			return false
		}

		unit := &processUnit{
			cacheKeys:   map[string]*cacheKey{},
			minheap:     util.NewMinHeap(65535),
			sqlLoader_:  newSqlLoader(loadDB, lname),
			sqlUpdater_: newSqlUpdater(writeBackDB, wname, sqlUpdateWg),
		}
		processUnits[i] = unit

		go ping(unit.sqlLoader_.queue)
		go ping(unit.sqlUpdater_.queue)
		go unit.kickCacheKey()
		go unit.sqlLoader_.run()
		go unit.sqlUpdater_.run()

	}

	return true
}

func StopProcessUnit() {
	for i := 0; i < conf.DefConfig.CacheGroupSize; i++ {
		processUnits[i].sqlUpdater_.queue.Close()
	}
	//time.Sleep(time.Second * 5)
	sqlUpdateWg.Wait()
}
