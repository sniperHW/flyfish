package server

import (
	"container/list"
	"github.com/sniperHW/flyfish/proto"
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

type kv struct {
	table           string
	uniKey          string
	key             string
	version         int64
	status          int
	cmdQueueLocked  bool //操作是否被锁定
	mtx             sync.Mutex
	cmdQueue        *list.List
	meta            *table_meta
	sqlFlag         int
	snapshoted      bool //当前key是否建立过快照
	m               *kvstore
	nnext           *kv
	pprev           *kv
	values          map[string]*proto.Field //关联的字段
	modifyFields    map[string]bool         //发生变更尚未更新到sql数据库的字段
	writeBackLocked bool                    //是否已经提交sql回写处理
	make_snapshot   bool                    //费否处于快照处理过程中
}

func (this *kv) lockCmdQueue() {
	if !this.cmdQueueLocked {
		this.cmdQueueLocked = true
	}
}

func (this *kv) unlockCmdQueue() {
	this.cmdQueueLocked = false
}

func (this *kv) kickAble() bool {
	defer this.mtx.Unlock()
	this.mtx.Lock()

	if this.make_snapshot {
		return false
	}

	if this.writeBackLocked {
		return false
	}

	if this.cmdQueueLocked {
		return false
	}

	if this.cmdQueue.Len() != 0 {
		return false
	}

	return true
}

func (this *kv) setMissing() {
	defer this.mtx.Unlock()
	this.mtx.Lock()
	this.setMissingNoLock()
}

func (this *kv) setMissingNoLock() {
	this.version = 0
	this.status = cache_missing
	this.values = nil
	this.snapshoted = false
	this.modifyFields = map[string]bool{}
}

func (this *kv) setOK(version int64) {
	defer this.mtx.Unlock()
	this.mtx.Lock()
	this.setOKNoLock(version)
}

func (this *kv) setOKNoLock(version int64) {
	this.version = version
	this.status = cache_ok
}

func (this *kv) pushCmd(cmd *command) {
	defer this.mtx.Unlock()
	this.mtx.Lock()
	this.cmdQueue.PushBack(cmd)
}

func (this *kv) getMeta() *table_meta {
	return (*table_meta)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&this.meta))))
}

func (this *kv) convertStr(fieldName string, value string) *proto.Field {

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

func (this *kv) setDefaultValue() {
	defer this.mtx.Unlock()
	this.mtx.Lock()
	this.setDefaultValueNoLock()
}

func (this *kv) setDefaultValueNoLock() {
	this.values = map[string]*proto.Field{}
	meta := this.getMeta()
	for _, v := range meta.fieldMetas {
		defaultV := proto.PackField(v.name, v.defaultV)
		this.values[v.name] = defaultV
	}
}

func (this *kv) setValueNoLock(ctx *cmdContext) {
	this.values = map[string]*proto.Field{}
	for _, v := range ctx.fields {

		Debugln("setValue", v.GetName())

		if !(v.GetName() == "__version__" || v.GetName() == "__key__") {
			this.values[v.GetName()] = v
		}
	}
}

func (this *kv) processClientCmd() {
	processCmd(this, true)
}

func (this *kv) processQueueCmd() {
	processCmd(this, false)
}

func newCacheKey(m *kvstore, table string, key string, uniKey string) *kv {

	meta := getMetaByTable(table)

	if nil == meta {
		Errorln("newCacheKey key:", uniKey, " error,[missing table_meta]")
		return nil
	}

	return &kv{
		uniKey:       uniKey,
		key:          key,
		status:       cache_new,
		meta:         meta,
		cmdQueue:     list.New(),
		m:            m,
		table:        table,
		modifyFields: map[string]bool{},
	}
}
