package flyfish

import (
	"container/list"
	"flyfish/conf"
	"flyfish/proto"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sniperHW/kendynet/util"
)

const (
	cache_new     = 1
	cache_ok      = 2
	cache_missing = 3
)

var (
	cacheGroup []*cacheKeyMgr
	tick       int64 //每次访问+1假设每秒访问100万次，需要运行584942年才会回绕
)

type cacheKey struct {
	uniKey       string
	idx          uint32
	version      int64
	status       int
	locked       bool //操作是否被锁定
	mtx          sync.Mutex
	cmdQueue     *list.List
	meta         *table_meta
	writeBacked  bool //正在回写
	writeBackVer int64
	lastAccess   int64
}

type cacheKeyMgr struct {
	cacheKeys map[string]*cacheKey
	minheap   *util.MinHeap
	mtx       sync.Mutex
}

func (this *cacheKey) Less(o util.HeapElement) bool {
	return this.lastAccess < o.(*cacheKey).lastAccess
}

func (this *cacheKey) GetIndex() uint32 {
	return this.idx
}

func (this *cacheKey) SetIndex(idx uint32) {
	this.idx = idx
}

func (this *cacheKey) lock() {
	if !this.locked {
		this.locked = true
	}
}

func (this *cacheKey) unlock() {
	this.locked = false
}

func (this *cacheKey) setMissing() {
	defer this.mtx.Unlock()
	this.mtx.Lock()
	Debugln("SetMissing key:", this.uniKey)
	this.version = 0
	this.status = cache_missing
}

func (this *cacheKey) setOK(version int64) {
	defer this.mtx.Unlock()
	this.mtx.Lock()
	this.version = version
	this.status = cache_ok
}

func (this *cacheKey) reset() {
	defer this.mtx.Unlock()
	this.mtx.Lock()
	this.status = cache_new
}

func (this *cacheKey) clearCmd() {
	defer this.mtx.Unlock()
	this.mtx.Lock()
	this.locked = false
	atomic.AddInt32(&cmdCount, -int32(this.cmdQueue.Len()))
	this.cmdQueue = list.New()
}

func (this *cacheKey) setWriteBack() {
	defer this.mtx.Unlock()
	this.mtx.Lock()
	this.writeBacked = true
	this.writeBackVer++
}

func (this *cacheKey) clearWriteBack(ver int64) {
	defer this.mtx.Unlock()
	this.mtx.Lock()
	if this.writeBackVer == ver {
		this.writeBacked = false
	}
}

func (this *cacheKey) isWriteBack() bool {
	defer this.mtx.Unlock()
	this.mtx.Lock()
	return this.writeBacked
}

func (this *cacheKey) updateLRU(minheap *util.MinHeap) {
	this.lastAccess = atomic.AddInt64(&tick, 1)
	minheap.Insert(this)
}

func newCacheKey(table string, uniKey string) *cacheKey {

	meta := getMetaByTable(table)

	if nil == meta {
		Errorln("newCacheKey key:", uniKey, " error,[missing table_meta]")
		return nil
	}

	return &cacheKey{
		uniKey:   uniKey,
		status:   cache_new,
		meta:     meta,
		cmdQueue: list.New(),
	}
}

func (this *cacheKey) convertStr(fieldName string, value string) *proto.Field {

	if fieldName == "__version__" {
		i, err := strconv.ParseInt(value, 10, 64)
		if nil != err {
			return nil
		}
		return proto.PackField(fieldName, i)
	}

	m, ok := this.meta.fieldMetas[fieldName]
	if !ok {
		return nil
	}

	if m.tt == proto.ValueType_string {
		return proto.PackField(fieldName, value)
	} else if m.tt == proto.ValueType_blob {
		return proto.PackField(fieldName, ([]byte)(value))
	} else if m.tt == proto.ValueType_float {
		f, err := strconv.ParseFloat(value, 64)
		if nil != err {
			return nil
		}
		return proto.PackField(fieldName, f)
	} else if m.tt == proto.ValueType_int {
		i, err := strconv.ParseInt(value, 10, 64)
		if nil != err {
			return nil
		}
		return proto.PackField(fieldName, i)
	} else if m.tt == proto.ValueType_uint {
		u, err := strconv.ParseUint(value, 10, 64)
		if nil != err {
			return nil
		}
		return proto.PackField(fieldName, u)
	} else {
		return nil
	}
}

func getCacheKey(table string, uniKey string) *cacheKey {
	mgr := getMgrByUnikey(uniKey)
	defer mgr.mtx.Unlock()
	mgr.mtx.Lock()
	k, ok := mgr.cacheKeys[uniKey]
	if ok {
		k.updateLRU(mgr.minheap)
	} else {
		k = newCacheKey(table, uniKey)
		if nil != k {
			k.updateLRU(mgr.minheap)
			mgr.cacheKeys[uniKey] = k
		}
	}
	return k
}

func getMgrByUnikey(uniKey string) *cacheKeyMgr {
	return cacheGroup[StringHash(uniKey)%conf.CacheGroupSize]
}

func kickCacheKey(mgr *cacheKeyMgr) {

	for !isStop() {
		time.Sleep(time.Second)
		mgr.mtx.Lock()
		for len(mgr.cacheKeys) > conf.MaxCachePerGroupSize {
			min := mgr.minheap.Min()
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
			mgr.minheap.PopMin()
			delete(mgr.cacheKeys, c.uniKey)

			cmd := &command{
				uniKey: c.uniKey,
				ckey:   c,
			}

			ctx := &processContext{
				commands:  []*command{cmd},
				redisFlag: redis_kick,
			}

			pushRedisNoWait(ctx)

		}

		//Infoln("cacheKeys size", len(mgr.cacheKeys))

		mgr.mtx.Unlock()
	}

}

func InitCacheKey() {
	cacheGroup = make([]*cacheKeyMgr, conf.CacheGroupSize)
	for i := 0; i < conf.CacheGroupSize; i++ {
		cacheGroup[i] = &cacheKeyMgr{
			cacheKeys: map[string]*cacheKey{},
			minheap:   util.NewMinHeap(65535),
		}

		go kickCacheKey(cacheGroup[i])

	}
}
