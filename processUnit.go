package flyfish

import (
	"container/list"
	"fmt"
	"github.com/jmoiron/sqlx"
	"github.com/sniperHW/flyfish/conf"
	//"github.com/sniperHW/flyfish/proto"
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

var cmdProcessor cmdProcessorI

var fnKickCacheKey func(*processUnit)

type cmdProcessorI interface {
	processCmd(*cacheKey, bool)
}

type processUnit struct {
	cacheKeys  map[string]*cacheKey
	mtx        sync.Mutex
	sqlLoader_ *sqlLoader
	writeBack  *writeBackProcessor
	lruHead    cacheKey
	lruTail    cacheKey
}

/*
func (this *processUnit) updateQueueFull() bool {
	return this.sqlUpdater_.queue.Full()
}
*/

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
	err := this.sqlLoader_.queue.AddNoWait(ctx, fullReturn...)
	if nil == err {
		return true
	} else {
		return false
	}
}

func (this *processUnit) onRedisStale(ckey *cacheKey, ctx *processContext) {
	/*  redis中的数据与flyfish key不一致
	 *  将ckey重置为cache_new，强制从数据库取值刷新redis
	 */
	ckey.mtx.Lock()
	defer ckey.mtx.Unlock()
	ckey.status = cache_new
	if ckey.writeBacked {
		ckey.cmdQueueLocked = false
		//尚未处理的cmd以及ctx中包含的cmd都不做响应，所以需要扣除正确的cmdCount
		atomic.AddInt32(&cmdCount, -int32(ckey.cmdQueue.Len()+len(ctx.commands)))
		ckey.cmdQueue = list.New()
	} else {
		ctx.writeBackFlag = write_back_none //数据存在执行update
		ctx.redisFlag = redis_none
		this.sqlLoader_.queue.AddNoWait(ctx)
	}
}

func (this *processUnit) doWriteBack(ctx *processContext) {

	Debugln("doWriteBack")

	if ctx.writeBackFlag == write_back_none {
		panic("ctx.writeBackFlag == write_back_none")
	}

	this.writeBack.writeBack(ctx)
}

func (this *cacheKey) process_(fromClient bool) {
	cmdProcessor.processCmd(this, fromClient)
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

//本地cache只需要删除key
func kickCacheKeyLocalCache(unit *processUnit) {
	MaxCachePerGroupSize := conf.GetConfig().MaxCachePerGroupSize

	for len(unit.cacheKeys) > MaxCachePerGroupSize && unit.lruHead.nnext != &unit.lruTail {

		c := unit.lruTail.pprev

		kickAble, _ := c.kickAble()

		if !kickAble {
			break
		}

		unit.removeLRU(c)
		delete(unit.cacheKeys, c.uniKey)
	}
}

//redis cache除了要剔除key还要根据key的状态剔除redis中的缓存
func kickCacheKeyRedisCache(unit *processUnit) {
	MaxCachePerGroupSize := conf.GetConfig().MaxCachePerGroupSize

	for len(unit.cacheKeys) > MaxCachePerGroupSize && unit.lruHead.nnext != &unit.lruTail {

		c := unit.lruTail.pprev

		kickAble, status := c.kickAble()

		if !kickAble {
			break
		}

		unit.removeLRU(c)
		delete(unit.cacheKeys, c.uniKey)

		if status == cache_ok {
			cmd := &command{
				uniKey: c.uniKey,
				ckey:   c,
			}

			ctx := &processContext{
				commands:  []*command{cmd},
				redisFlag: redis_kick,
			}

			unit.pushRedisReq(ctx)
		}
	}
}

func (this *processUnit) kickCacheKey() {
	fnKickCacheKey(this)
}

func InitProcessUnit() bool {

	//placeHolderInit()

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
			cacheKeys:  map[string]*cacheKey{},
			sqlLoader_: newSqlLoader(loadDB, lname),
			writeBack: &writeBackProcessor{
				sqlUpdater_: newSqlUpdater(writeBackDB, wname, sqlUpdateWg, false),
			},
		}

		unit.lruHead.nnext = &unit.lruTail
		unit.lruTail.pprev = &unit.lruHead
		unit.writeBack.start()

		processUnits[i] = unit

		go unit.sqlLoader_.run()
		go unit.writeBack.sqlUpdater_.run()

		timer.Repeat(time.Second*60, nil, func(t *timer.Timer) {
			if isStop() || util.ErrQueueClosed == unit.sqlLoader_.queue.AddNoWait(&processContext{ping: true}) {
				t.Cancel()
			}
		})

		timer.Repeat(time.Second*60, nil, func(t *timer.Timer) {
			if isStop() || util.ErrQueueClosed == unit.writeBack.sqlUpdater_.queue.AddNoWait(int64(-1)) {
				t.Cancel()
			}
		})

		timer.Repeat(time.Millisecond*100, nil, func(t *timer.Timer) {
			if isStop() {
				t.Cancel()
			} else {
				unit.writeBack.checkFlush()
			}
		})

		/*timer.Repeat(time.Second, nil, func(t *timer.Timer) {
			if isStop() {
				t.Cancel()
			} else {
				Infoln(wname, unit.sqlUpdater_.queue.Len())
			}
		})*/

	}

	return true
}

func StopProcessUnit() {
	config := conf.GetConfig()
	for i := 0; i < config.CacheGroupSize; i++ {
		processUnits[i].writeBack.sqlUpdater_.queue.Close()
	}
	sqlUpdateWg.Wait()
}

func updateSqlUpdateQueueSize(SqlUpdateQueueSize int) {
	for _, v := range processUnits {
		v.writeBack.sqlUpdater_.queue.SetFullSize(SqlUpdateQueueSize)
	}
}

func updateSqlLoadQueueSize(SqlLoadQueueSize int) {
	for _, v := range processUnits {
		v.writeBack.sqlUpdater_.queue.SetFullSize(SqlLoadQueueSize)
	}
}
