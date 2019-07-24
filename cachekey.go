package flyfish

import (
	"container/list"
	"flyfish/proto"
	"strconv"
	"sync"
	"sync/atomic"
	"unsafe"
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
	uniKey         string
	version        int64
	status         int
	cmdQueueLocked bool //操作是否被锁定
	mtx            sync.Mutex
	cmdQueue       *list.List
	meta           *table_meta
	writeBacked    bool //正在回写
	unit           *processUnit
	r              *record
	nnext          *cacheKey
	pprev          *cacheKey
}

func (this *cacheKey) lockCmdQueue() {
	if !this.cmdQueueLocked {
		this.cmdQueueLocked = true
	}
}

func (this *cacheKey) unlockCmdQueue() {
	this.cmdQueueLocked = false
}

func (this *cacheKey) kickAble() (bool, int) {
	defer this.mtx.Unlock()
	this.mtx.Lock()
	if this.cmdQueueLocked {
		return false, this.status
	}

	if this.writeBacked {
		return false, this.status
	}

	if this.cmdQueue.Len() != 0 {
		return false, this.status
	}

	return true, this.status
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
	this.cmdQueueLocked = false
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

func (this *cacheKey) pushCmd(cmd *command) {
	defer this.mtx.Unlock()
	this.mtx.Lock()
	this.cmdQueue.PushBack(cmd)
}

func (this *cacheKey) getMeta() *table_meta {
	return (*table_meta)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&this.meta))))
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
