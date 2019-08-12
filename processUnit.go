package flyfish

import (
	//"container/list"
	//"fmt"
	//"github.com/jmoiron/sqlx"
	"github.com/sniperHW/flyfish/conf"
	//"github.com/sniperHW/flyfish/proto"
	"github.com/sniperHW/kendynet/timer"
	//"github.com/sniperHW/kendynet/util"
	"sync"
	//"sync/atomic"
	"time"
)

/*
 *    每个processUnit负责处理其关联的key
 */

var CacheGroupSize int

var processUnits []*processUnit

var cmdProcessor cmdProcessorI

var fnKickCacheKey func(*processUnit)

type cmdProcessorI interface {
	processCmd(*cacheKey, bool)
}

type processUnit struct {
	cacheKeys map[string]*cacheKey
	mtx       sync.Mutex
	writeBack *writeBackProcessor
	lruHead   cacheKey
	lruTail   cacheKey
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

			pushRedisReq(ctx)
		}
	}
}

func (this *processUnit) kickCacheKey() {
	fnKickCacheKey(this)
}

func initProcessUnit() {

	config := conf.GetConfig()

	CacheGroupSize = config.CacheGroupSize

	processUnits = make([]*processUnit, CacheGroupSize)
	for i := 0; i < CacheGroupSize; i++ {

		unit := &processUnit{
			cacheKeys: map[string]*cacheKey{},
			writeBack: &writeBackProcessor{
				id: i,
			},
		}

		unit.lruHead.nnext = &unit.lruTail
		unit.lruTail.pprev = &unit.lruHead
		unit.writeBack.start()

		processUnits[i] = unit

		timer.Repeat(time.Millisecond*100, nil, func(t *timer.Timer) {
			if isStop() {
				t.Cancel()
			} else {
				unit.writeBack.checkFlush()
			}
		})

	}
}
