package flyfish

import (
	"container/list"
	"flyfish/proto"
	"github.com/sniperHW/kendynet/util"
	"strconv"
	"sync"
	"sync/atomic"
)

const (
	cache_new     = 1
	cache_ok      = 2
	cache_missing = 3
)

type record struct {
	writeBackFlag int
	key           string
	table         string
	uniKey        string
	ckey          *cacheKey
	fields        map[string]*proto.Field //所有命令的字段聚合
	ctx           *processContext
}

var recordPool = sync.Pool{
	New: func() interface{} {
		return &record{}
	},
}

func recordGet() *record {
	r := recordPool.Get().(*record)
	return r
}

func recordPut(r *record) {
	r.writeBackFlag = write_back_none
	r.ckey = nil
	r.fields = nil
	r.ctx = nil
	recordPool.Put(r)
}

type cacheKey struct {
	uniKey      string
	idx         uint32
	version     int64
	status      int
	locked      bool //操作是否被锁定
	mtx         sync.Mutex
	cmdQueue    *list.List
	meta        *table_meta
	writeBacked bool //正在回写
	lastAccess  int64
	unit        *processUnit
	r           *record
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

func (this *cacheKey) clearWriteBack() {
	defer this.mtx.Unlock()
	this.mtx.Lock()
	this.writeBacked = false
	if nil != this.r {
		recordPut(this.r)
		this.r = nil
	}
}

func (this *cacheKey) getRecord() *record {
	defer this.mtx.Unlock()
	this.mtx.Lock()
	return this.r
}

func (this *cacheKey) isWriteBack() bool {
	defer this.mtx.Unlock()
	this.mtx.Lock()
	return this.writeBacked
}

func (this *cacheKey) updateLRU() {
	this.lastAccess = atomic.AddInt64(&this.unit.tick, 1)
	this.unit.minheap.Insert(this)
}

func newCacheKey(unit *processUnit, table string, uniKey string) *cacheKey {

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
		unit:     unit,
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
