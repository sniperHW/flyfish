package flyfish

import (
	"container/list"
	"flyfish/conf"
	protocol "flyfish/proto"
	"github.com/sniperHW/kendynet/event"
	"strconv"
	"sync/atomic"
)

const (
	cache_new     = 1
	cache_ok      = 2
	cache_missing = 3
)

type cacheKey struct {
	uniKey string
	idx    uint32
	//lastAccess  int64              //time.Time
	version     int64
	status      int
	locked      bool //操作是否被锁定
	cmdQueue    *list.List
	meta        *table_meta
	writeBacked int32 //正在回写
}

//var tick int64  //每次访问+1假设每秒访问100万次，需要运行584942年才会回绕

type cacheKeyMgr struct {
	cacheKeys  map[string]*cacheKey
	eventQueue *event.EventQueue
}

var cacheGroup []*cacheKeyMgr

func (this *cacheKey) lock() {
	if !this.locked {
		this.locked = true
	}
}

func (this *cacheKey) unlock() {
	this.locked = false
}

func (this *cacheKey) setMissing() {
	Debugln("SetMissing key:", this.uniKey)
	this.version = 0
	this.status = cache_missing
}

func (this *cacheKey) setOK(version int64) {
	this.version = version
	this.status = cache_ok
}

func (this *cacheKey) reset() {
	this.status = cache_new
}

func (this *cacheKey) pushCmd(cmd *command) {
	this.cmdQueue.PushBack(cmd)
}

func (this *cacheKey) clearCmd() {
	this.cmdQueue = list.New()
}

func (this *cacheKey) setWriteBack() {
	atomic.StoreInt32(&this.writeBacked, 1)
}

func (this *cacheKey) clearWriteBack() {
	atomic.StoreInt32(&this.writeBacked, 0)
}

func (this *cacheKey) isWriteBack() bool {
	return atomic.LoadInt32(&this.writeBacked) == 1
}

/*
UpdateLRU和newCacheKey只能再主消息循环中访问，所以tick不需要加锁保护
*/

func (this *cacheKey) updateLRU() {
	//atomic.AddInt64
}

func newCacheKey(table string, uniKey string) *cacheKey {

	meta := getMetaByTable(table)

	if nil == meta {
		Errorln("newCacheKey key:", uniKey, " error,[missing table_meta]")
		return nil
	}

	k := &cacheKey{
		uniKey:   uniKey,
		status:   cache_new,
		meta:     meta,
		cmdQueue: list.New(),
	}

	Debugln("newCacheKey key:", uniKey)

	mgr := getMgrByUnikey(uniKey)

	mgr.cacheKeys[uniKey] = k

	return k
}

func getCacheKey(table string, uniKey string) *cacheKey {
	mgr := getMgrByUnikey(uniKey)
	k, ok := mgr.cacheKeys[uniKey]
	if ok {
		k.updateLRU()
		return k
	} else {
		return newCacheKey(table, uniKey)
	}
}

func (this *cacheKey) convertStr(fieldName string, value string) *protocol.Field {
	m, ok := this.meta.fieldMetas[fieldName]
	if !ok {
		return nil
	}

	if m.tt == protocol.ValueType_string {
		return protocol.PackField(fieldName, value)
	} else if m.tt == protocol.ValueType_float {
		f, err := strconv.ParseFloat(value, 64)
		if nil != err {
			return nil
		}
		return protocol.PackField(fieldName, f)
	} else if m.tt == protocol.ValueType_int {
		i, err := strconv.ParseInt(value, 10, 64)
		if nil != err {
			return nil
		}
		return protocol.PackField(fieldName, i)
	} else if m.tt == protocol.ValueType_uint {
		u, err := strconv.ParseUint(value, 10, 64)
		if nil != err {
			return nil
		}
		return protocol.PackField(fieldName, u)
	} else {
		return nil
	}
}

func getMgrByUnikey(uniKey string) *cacheKeyMgr {
	hash := StringHash(uniKey)
	return cacheGroup[hash%conf.CacheGroupSize]
}

func postKeyEventNoWait(uniKey string, op interface{}, args ...interface{}) {
	getMgrByUnikey(uniKey).eventQueue.PostNoWait(op, args...)
}

func postKeyEvent(uniKey string, op interface{}, args ...interface{}) {
	getMgrByUnikey(uniKey).eventQueue.Post(op, args...)
}

func InitCacheKey() {
	cacheGroup = make([]*cacheKeyMgr, conf.CacheGroupSize)
	for i := 0; i < conf.CacheGroupSize; i++ {
		eventQueue := event.NewEventQueue(conf.MainEventQueueSize)
		cacheGroup[i] = &cacheKeyMgr{
			cacheKeys:  map[string]*cacheKey{},
			eventQueue: eventQueue,
		}
		go func() {
			eventQueue.Run()
		}()
	}
}
