package server

import (
	"github.com/sniperHW/flyfish/conf"
	"github.com/sniperHW/kendynet/timer"
	"github.com/sniperHW/kendynet/util"
	"os"
	"sync"
	"time"
)

var CacheGroupSize int

var cacheMgrs []*cacheMgr

var cmdProcessor cmdProcessorI

type cmdProcessorI interface {
	processCmd(*cacheKey, bool)
}

type ctxArray struct {
	count int
	ctxs  []*cmdContext
}

func (this *ctxArray) append(ctx *cmdContext) {
	//FlushCount可能因为重载配置文件变大，导致原有空间不足
	if this.count >= len(this.ctxs) {
		ctxs := make([]*cmdContext, conf.GetConfig().FlushCount)
		copy(ctxs, this.ctxs)
		this.ctxs = ctxs
	}
	this.ctxs[this.count] = ctx
	this.count++
}

func (this *ctxArray) full() bool {
	return this.count == cap(this.ctxs)
}

var ctxArrayPool = sync.Pool{
	New: func() interface{} {
		return &ctxArray{
			ctxs:  make([]*cmdContext, conf.GetConfig().FlushCount),
			count: 0,
		}
	},
}

func ctxArrayGet() *ctxArray {
	return ctxArrayPool.Get().(*ctxArray)
}

func ctxArrayPut(w *ctxArray) {
	w.count = 0
	ctxArrayPool.Put(w)
}

type cacheMgr struct {
	kv               map[string]*cacheKey
	mtx              sync.Mutex
	lruHead          cacheKey
	lruTail          cacheKey
	ctxs             *ctxArray
	nextFlush        time.Time
	binlogStr        *str
	f                *os.File
	filePath         string
	backFilePath     string
	binlogCount      int32 //当前binlog文件总binlog数量
	cacheBinlogCount int32 //待序列化到文件的binlog数量
	fileSize         int   //当前binlog文件大小
	make_snapshot    bool  //当前是否正在建立快照
	binlogQueue      *util.BlockQueue
}

func (this *cacheMgr) doWriteBack(ctx *cmdContext) {
	this.writeBack(ctx)
}

func getMgrByUnikey(uniKey string) *cacheMgr {
	return cacheMgrs[StringHash(uniKey)%CacheGroupSize]
}

func (this *cacheMgr) updateLRU(ckey *cacheKey) {

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

func (this *cacheMgr) removeLRU(ckey *cacheKey) {
	ckey.pprev.nnext = ckey.nnext
	ckey.nnext.pprev = ckey.pprev
	ckey.nnext = nil
	ckey.pprev = nil
}

func (this *cacheMgr) kickCacheKey() {
	MaxCachePerGroupSize := conf.GetConfig().MaxCachePerGroupSize

	for len(this.kv) > MaxCachePerGroupSize && this.lruHead.nnext != &this.lruTail {

		c := this.lruTail.pprev

		if !c.kickAble() {
			return
		}

		this.removeLRU(c)
		this.writeKick(c.uniKey)
		delete(this.kv, c.uniKey)
	}
}

func initCacheMgr() {

	config := conf.GetConfig()

	CacheGroupSize = config.CacheGroupSize

	cacheMgrs = make([]*cacheMgr, CacheGroupSize)
	for i := 0; i < CacheGroupSize; i++ {

		m := &cacheMgr{
			kv:        map[string]*cacheKey{},
			nextFlush: time.Now().Add(time.Millisecond * time.Duration(config.FlushInterval)),
		}

		m.lruHead.nnext = &m.lruTail
		m.lruTail.pprev = &m.lruHead

		timer.Repeat(time.Millisecond*time.Duration(config.FlushInterval), nil, func(t *timer.Timer) {
			if isStop() {
				t.Cancel()
			} else {
				m.mtx.Lock()
				m.tryFlush()
				m.mtx.Unlock()
			}
		})

		timer.Repeat(time.Second, nil, func(t *timer.Timer) {
			if isStop() {
				t.Cancel()
			} else {
				m.mtx.Lock()
				m.kickCacheKey()
				m.mtx.Unlock()
			}
		})

		m.start()

		cacheMgrs[i] = m
	}
}
