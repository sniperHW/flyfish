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

type writeBackRecord struct {
	writeBackFlag int
	key           string
	table         string
	uniKey        string
	ckey          *cacheKey
	fields        map[string]*proto.Field //所有命令的字段聚合
	ctx           *processContext
}

/*var recordPool = sync.Pool{
	New: func() interface{} {
		return &writeBackRecord{}
	},
}

func recordGet() *writeBackRecord {
	r := recordPool.Get().(*writeBackRecord)
	return r
}

func recordPut(r *writeBackRecord) {
	r.writeBackFlag = write_back_none
	r.ckey = nil
	r.fields = nil
	r.ctx = nil
	recordPool.Put(r)
}*/

type cacheKey struct {
	uniKey         string
	key            string
	version        int64
	status         int
	cmdQueueLocked bool //操作是否被锁定
	mtx            sync.Mutex
	cmdQueue       *list.List
	meta           *table_meta
	writeBacked    bool //正在回写
	unit           *processUnit
	r              *writeBackRecord
	nnext          *cacheKey
	pprev          *cacheKey
	values         map[string]*proto.Field
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
	this.version = 0
	this.status = cache_missing
	this.values = nil

}

func (this *cacheKey) setMissingNoLock() {
	this.version = 0
	this.status = cache_missing
	this.values = nil
}

func (this *cacheKey) setOK(version int64) {
	defer this.mtx.Unlock()
	this.mtx.Lock()
	this.version = version
	this.status = cache_ok
}

func (this *cacheKey) setOKNoLock(version int64) {
	this.version = version
	this.status = cache_ok
}

func (this *cacheKey) clearWriteBack() {
	defer this.mtx.Unlock()
	this.mtx.Lock()
	this.writeBacked = false
}

func (this *cacheKey) getRecord() *writeBackRecord {
	defer this.mtx.Unlock()
	this.mtx.Lock()
	r := this.r
	this.r = nil
	return r
}

func (this *cacheKey) pushCmd(cmd *command) {
	defer this.mtx.Unlock()
	this.mtx.Lock()
	this.cmdQueue.PushBack(cmd)
}

func (this *cacheKey) getMeta() *table_meta {
	return (*table_meta)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&this.meta))))
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

func newCacheKey(unit *processUnit, table string, key string, uniKey string) *cacheKey {

	meta := getMetaByTable(table)

	if nil == meta {
		Errorln("newCacheKey key:", uniKey, " error,[missing table_meta]")
		return nil
	}

	return &cacheKey{
		uniKey:   uniKey,
		key:      key,
		status:   cache_new,
		meta:     meta,
		cmdQueue: list.New(),
		unit:     unit,
	}
}
