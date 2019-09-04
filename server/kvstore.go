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

var kvstoreMgr []*kvstore

var cmdProcessor cmdProcessorI

type cmdProcessorI interface {
	processCmd(*kv, bool)
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

type kvstore struct {
	kv            map[string]*kv
	mtx           sync.Mutex
	lruHead       kv
	lruTail       kv
	ctxs          *ctxArray
	nextFlush     time.Time
	binlogStr     *str
	f             *os.File
	filePath      string
	backFilePath  string
	binlogCount   int32 //当前binlog文件总binlog数量
	batchCount    int32 //待序列化到文件的binlog数量
	fileSize      int   //当前binlog文件大小
	make_snapshot bool  //当前是否正在建立快照
	binlogQueue   *util.BlockQueue
}

func getKvstore(uniKey string) *kvstore {
	return kvstoreMgr[StringHash(uniKey)%CacheGroupSize]
}

func (this *kvstore) updateLRU(ckey *kv) {

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

func (this *kvstore) removeLRU(ckey *kv) {
	ckey.pprev.nnext = ckey.nnext
	ckey.nnext.pprev = ckey.pprev
	ckey.nnext = nil
	ckey.pprev = nil
}

func (this *kvstore) kickCacheKey() {
	MaxCachePerGroupSize := conf.GetConfig().MaxCachePerGroupSize

	for len(this.kv) > MaxCachePerGroupSize && this.lruHead.nnext != &this.lruTail {

		c := this.lruTail.pprev

		if !c.kickAble() {
			return
		}

		this.removeLRU(c)
		this.appendBinlog(binlog_kick, c.uniKey, nil, 0)
		this.tryBatchWrite()
		delete(this.kv, c.uniKey)
	}
}

func initKvStore() bool {
	config := conf.GetConfig()

	CacheGroupSize = config.CacheGroupSize

	kvstoreMgr = make([]*kvstore, CacheGroupSize)
	for i := 0; i < CacheGroupSize; i++ {

		m := &kvstore{
			kv:          map[string]*kv{},
			nextFlush:   time.Now().Add(time.Millisecond * time.Duration(config.FlushInterval)),
			binlogQueue: util.NewBlockQueue(),
		}

		m.lruHead.nnext = &m.lruTail
		m.lruTail.pprev = &m.lruHead

		kvstoreMgr[i] = m
	}

	if !StartReplayBinlog() {
		return false
	}

	for _, v := range kvstoreMgr {

		c := v

		timer.Repeat(time.Millisecond*time.Duration(config.FlushInterval), nil, func(t *timer.Timer) {
			if isStop() {
				t.Cancel()
			} else {
				c.mtx.Lock()
				c.tryBatchWrite()
				c.mtx.Unlock()
			}
		})

		timer.Repeat(time.Second, nil, func(t *timer.Timer) {
			if isStop() {
				t.Cancel()
			} else {
				c.mtx.Lock()
				c.kickCacheKey()
				c.mtx.Unlock()
			}
		})

		go func() {
			for {
				closed, localList := c.binlogQueue.Get()
				for _, v := range localList {
					switch v.(type) {
					case *binlogBatch:
						st := v.(*binlogBatch)
						c.batchWriteBinlog(st.binlogStr, st.ctxs, st.batchCount)
					default:
						v.(func())()
					}
				}
				if closed {
					return
				}
			}
		}()
	}

	return true

}
