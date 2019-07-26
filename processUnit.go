package flyfish

import (
	"container/list"
	"flyfish/conf"
	"flyfish/errcode"
	"flyfish/proto"
	"fmt"
	"github.com/jmoiron/sqlx"
	"github.com/sniperHW/kendynet/timer"
	"github.com/sniperHW/kendynet/util"
	"sync"
	"sync/atomic"
	"time"
)

/*
 *    每个processUnit负责处理其关联的key
 */

var CacheGroupSize int

var processUnits []*processUnit

var sqlUpdateWg *sync.WaitGroup

type processUnit struct {
	cacheKeys   map[string]*cacheKey
	mtx         sync.Mutex
	sqlLoader_  *sqlLoader
	sqlUpdater_ *sqlUpdater
	lruHead     cacheKey
	lruTail     cacheKey
}

func (this *processUnit) updateQueueFull() bool {
	return this.sqlUpdater_.queue.Full()
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
	return nil == this.sqlLoader_.queue.AddNoWait(ctx, fullReturn...)
}

func (this *processUnit) pushSqlLoadReqOnRedisReply(ctx *processContext) bool {
	ckey := ctx.getCacheKey()
	ckey.mtx.Lock()
	if ckey.writeBacked {
		ckey.cmdQueueLocked = false
		atomic.AddInt32(&cmdCount, -int32(ckey.cmdQueue.Len()))
		ckey.cmdQueue = list.New()
		ckey.mtx.Unlock()
		return false
	} else {
		this.sqlLoader_.queue.AddNoWait(ctx)
		return true
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

func (this *processUnit) pushSqlWriteBackReq(ctx *processContext) {

	if ctx.writeBackFlag == write_back_none {
		panic("ctx.writeBackFlag == write_back_none")
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

		this.sqlUpdater_.queue.AddNoWait(ckey)

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
				meta := wb.ckey.getMeta().fieldMetas
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
				meta := wb.ckey.getMeta().fieldMetas
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
}

func (this *cacheKey) process_(fromClient bool) {

	this.mtx.Lock()

	if !fromClient {
		this.unlockCmdQueue()
	}

	if this.cmdQueueLocked || this.cmdQueue.Len() == 0 {
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
			if conf.GetConfig().ReplyBusyOnQueueFull {
				ctx.reply(errcode.ERR_BUSY, nil, -1)
			} else {
				atomic.AddInt32(&cmdCount, -1)
			}
			this.process_(fromClient)
			return
		}
	}

	fullReturn := fromClient

	ok := true

	if this.status == cache_ok || this.status == cache_missing {
		ok = this.unit.pushRedisReq(ctx, fullReturn)
	} else {
		ok = this.unit.pushSqlLoadReq(ctx, fullReturn)
	}

	if !ok {
		this.mtx.Unlock()
		if conf.GetConfig().ReplyBusyOnQueueFull {
			ctx.reply(errcode.ERR_BUSY, nil, -1)
		} else {
			atomic.AddInt32(&cmdCount, -1)
		}
		this.process_(fromClient)
	} else {
		this.unlockCmdQueue()
		this.mtx.Unlock()
	}
}

func getUnitByUnikey(uniKey string) *processUnit {
	return processUnits[StringHash(uniKey)%CacheGroupSize]
}

func (this *processUnit) updateLRU(ckey *cacheKey) {

	if ckey.nnext != nil || ckey.pprev != nil {
		//先移除
		ckey.pprev.nnext = ckey.nnext
		ckey.nnext.pprev = ckey.pprev
		ckey.nnext = nil
		ckey.pprev = nil
	}

	//插入头部
	ckey.nnext = this.lruHead.nnext
	ckey.nnext.pprev = ckey
	ckey.pprev = &this.lruHead
	this.lruHead.nnext = ckey

}

func (this *processUnit) removeLRU(ckey *cacheKey) {
	ckey.pprev.nnext = ckey.nnext
	ckey.nnext.pprev = ckey.pprev
	ckey.nnext = nil
	ckey.pprev = nil
}

func (this *processUnit) kickCacheKey() {

	//defer this.mtx.Unlock()
	//this.mtx.Lock()

	MaxCachePerGroupSize := conf.GetConfig().MaxCachePerGroupSize

	for len(this.cacheKeys) > MaxCachePerGroupSize && this.lruHead.nnext != &this.lruTail {

		c := this.lruTail.pprev

		kickAble, status := c.kickAble()

		if !kickAble {
			break
		}

		this.removeLRU(c)
		delete(this.cacheKeys, c.uniKey)

		if status == cache_ok {
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
	}
}

func InitProcessUnit() bool {

	placeHolderInit()

	sqlUpdateWg = &sync.WaitGroup{}

	config := conf.GetConfig()

	CacheGroupSize = config.CacheGroupSize

	processUnits = make([]*processUnit, CacheGroupSize)
	for i := 0; i < CacheGroupSize; i++ {

		lname := fmt.Sprintf("sqlLoad:%d", i)
		wname := fmt.Sprintf("sqlUpdater:%d", i)

		var loadDB *sqlx.DB
		var writeBackDB *sqlx.DB
		var err error
		dbConfig := config.DBConfig

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
			sqlLoader_:  newSqlLoader(loadDB, lname),
			sqlUpdater_: newSqlUpdater(writeBackDB, wname, sqlUpdateWg),
		}

		unit.lruHead.nnext = &unit.lruTail
		unit.lruTail.pprev = &unit.lruHead

		processUnits[i] = unit

		go unit.sqlLoader_.run()
		go unit.sqlUpdater_.run()

		/*timer.Repeat(time.Second, nil, func(t *timer.Timer) {
			if isStop() {
				t.Cancel()
			} else {
				unit.kickCacheKey()
			}
		})*/

		timer.Repeat(time.Second*60, nil, func(t *timer.Timer) {
			if isStop() || util.ErrQueueClosed == unit.sqlLoader_.queue.AddNoWait(&processContext{ping: true}) {
				t.Cancel()
			}
		})

		timer.Repeat(time.Second*60, nil, func(t *timer.Timer) {
			if isStop() || util.ErrQueueClosed == unit.sqlUpdater_.queue.AddNoWait(&processContext{ping: true}) {
				t.Cancel()
			}
		})

		timer.Repeat(time.Second, nil, func(t *timer.Timer) {
			if isStop() {
				t.Cancel()
			} else {
				Infoln(wname, unit.sqlUpdater_.queue.Len())
			}
		})

	}

	return true
}

func StopProcessUnit() {
	config := conf.GetConfig()
	for i := 0; i < config.CacheGroupSize; i++ {
		processUnits[i].sqlUpdater_.queue.Close()
	}
	sqlUpdateWg.Wait()
}

func updateSqlUpdateQueueSize(SqlUpdateQueueSize int) {
	for _, v := range processUnits {
		v.sqlUpdater_.queue.SetFullSize(SqlUpdateQueueSize)
	}
}

func updateSqlLoadQueueSize(SqlLoadQueueSize int) {
	for _, v := range processUnits {
		v.sqlUpdater_.queue.SetFullSize(SqlLoadQueueSize)
	}
}
