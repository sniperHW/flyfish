package flyfish

import (
	"github.com/sniperHW/flyfish/conf"
	"github.com/sniperHW/kendynet/timer"
	"github.com/syndtr/goleveldb/leveldb"
	"sync"
	"time"
)

/*
 *    每个processUnit负责处理其关联的key
 */

var CacheGroupSize int

var processUnits []*processUnit

var cmdProcessor cmdProcessorI

type cmdProcessorI interface {
	processCmd(*cacheKey, bool)
}

type ctxArray struct {
	count int
	ctxs  []*processContext
}

func (this *ctxArray) append(ctx *processContext) {
	this.ctxs[this.count] = ctx
	this.count++
}

func (this *ctxArray) full() bool {
	return this.count == cap(this.ctxs)
}

func (this *ctxArray) reset() {
	this.count = 0
}

func (this *ctxArray) len() int {
	return this.count
}

type processUnit struct {
	cacheKeys    map[string]*cacheKey
	mtx          sync.Mutex
	lruHead      cacheKey
	lruTail      cacheKey
	levelDBBatch *leveldb.Batch
	ctxs         *ctxArray
	nextFlush    time.Time
}

func (this *processUnit) doWriteBack(ctx *processContext) {

	Debugln("doWriteBack")

	if ctx.writeBackFlag == write_back_none {
		panic("ctx.writeBackFlag == write_back_none")
	}

	this.writeBack(ctx)
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

func (this *processUnit) kickCacheKey() {
	/*MaxCachePerGroupSize := conf.GetConfig().MaxCachePerGroupSize

	for len(this.cacheKeys) > MaxCachePerGroupSize && this.lruHead.nnext != &this.lruTail {

		c := this.lruTail.pprev

		if c.kickAble() {
			break
		}

		this.removeLRU(c)
		delete(this.cacheKeys, c.uniKey)
	}*/
}

func (this *processUnit) checkFlush() {
	this.mtx.Lock()
	defer this.mtx.Unlock()
	if time.Now().After(this.nextFlush) {
		this.flushBatch()
	}
}

func initProcessUnit() {

	config := conf.GetConfig()

	CacheGroupSize = config.CacheGroupSize

	processUnits = make([]*processUnit, CacheGroupSize)
	for i := 0; i < CacheGroupSize; i++ {

		unit := &processUnit{
			cacheKeys:    map[string]*cacheKey{},
			levelDBBatch: new(leveldb.Batch),
			ctxs: &ctxArray{
				ctxs:  make([]*processContext, conf.GetConfig().FlushCount),
				count: 0,
			},
			nextFlush: time.Now().Add(time.Millisecond * time.Duration(config.FlushInterval)),
		}

		unit.lruHead.nnext = &unit.lruTail
		unit.lruTail.pprev = &unit.lruHead

		timer.Repeat(time.Millisecond*100, nil, func(t *timer.Timer) {
			if isStop() {
				t.Cancel()
			} else {
				unit.checkFlush()
			}
		})

		processUnits[i] = unit
	}
}
