package flyfish

import (
	"github.com/sniperHW/flyfish/errcode"
	"github.com/sniperHW/flyfish/proto"
	"sync/atomic"
	"time"
	"unsafe"
)

const (
	cmdNone            = 0
	cmdGet             = 1
	cmdSet             = 2
	cmdSetNx           = 3
	cmdCompareAndSet   = 4
	cmdCompareAndSetNx = 5
	cmdDel             = 6
	cmdIncrBy          = 7
	cmdDecrBy          = 8
)

var (
	cmdCount int32 //待回复命令数量
)

func checkReq(req *proto.ReqCommon) (bool, int32) {
	if isStop() {
		return false, errcode.ERR_SERVER_STOPED
	}

	if "" == req.GetTable() {
		return false, errcode.ERR_MISSING_TABLE
	}

	if "" == req.GetKey() {
		return false, errcode.ERR_MISSING_KEY
	}

	if nil == getMetaByTable(req.GetTable()) {
		return false, errcode.ERR_INVAILD_TABLE
	}

	return true, errcode.ERR_OK
}

func checkIncrDecrReq(req *proto.ReqCommon, field *proto.Field) (bool, int32) {
	if ok, errno := checkReq(req); !ok {
		return false, errno
	} else if nil == field {
		return false, errcode.ERR_MISSING_FIELDS
	} else {
		return true, errcode.ERR_OK
	}
}

func checkSetReq(req *proto.ReqCommon, fields []*proto.Field) (bool, int32) {
	if ok, errno := checkReq(req); !ok {
		return false, errno
	} else if 0 == len(fields) {
		return false, errcode.ERR_MISSING_FIELDS
	} else {
		return true, errcode.ERR_OK
	}
}

func checkCmpSetReq(req *proto.ReqCommon, newV *proto.Field, oldV *proto.Field) (bool, int32) {
	if ok, errno := checkReq(req); !ok {
		return false, errno
	} else if nil == newV || nil == oldV {
		return false, errcode.ERR_MISSING_FIELDS
	} else {
		return true, errcode.ERR_OK
	}
}

func isSetCmd(cmd int) bool {
	return cmd == cmdSet || cmd == cmdSetNx || cmd == cmdCompareAndSet || cmd == cmdCompareAndSetNx || cmd == cmdIncrBy || cmd == cmdDecrBy
}

//会导致会写的命令
func causeWriteBackCmd(cmd int) bool {
	return isSetCmd(cmd) || cmd == cmdDel
}

//命令回复器
type replyer interface {
	reply(errCode int32, fields map[string]*proto.Field, version int64)
}

type cnsSt struct {
	oldV *proto.Field
	newV *proto.Field
}

//来自客户端的一条命令请求
type command struct {
	next         *command
	cmdType      int
	rpyer        replyer
	table        string
	key          string
	uniKey       string //table+key
	version      *int64
	ckey         *cacheKey
	fields       map[string]*proto.Field //for get/set
	cns          *cnsSt                  //for compareAndSet
	incrDecr     *proto.Field            //for incr/decr
	deadline     time.Time
	respDeadline time.Time
	replyed      int32
}

func (this *command) reply(errCode int32, fields map[string]*proto.Field, version int64) {
	if atomic.CompareAndSwapInt32(&this.replyed, 0, 1) {
		this.rpyer.reply(errCode, fields, version)
		atomic.AddInt32(&cmdCount, -1)
	}
}

func (this *command) process() {

	atomic.AddInt32(&cmdCount, 1)

	meta := getMetaByTable(this.table)

	if nil == meta {
		this.reply(errcode.ERR_INVAILD_TABLE, nil, -1)
		return
	}

	if this.cmdType == cmdGet && !meta.checkGet(this.fields) {
		this.reply(errcode.ERR_INVAILD_FIELD, nil, -1)
		return
	}

	if (this.cmdType == cmdCompareAndSet || this.cmdType == cmdCompareAndSetNx) &&
		!meta.checkCompareAndSet(this.cns.newV, this.cns.oldV) {
		this.reply(errcode.ERR_INVAILD_FIELD, nil, -1)
		return
	}

	if (this.cmdType == cmdSet || this.cmdType == cmdSetNx) && !meta.checkSet(this.fields) {
		this.reply(errcode.ERR_INVAILD_FIELD, nil, -1)
		return
	}

	if (this.cmdType == cmdIncrBy || this.cmdType == cmdDecrBy) && !meta.checkField(this.incrDecr) {
		this.reply(errcode.ERR_INVAILD_FIELD, nil, -1)
		return
	}

	m := getMgrByUnikey(this.uniKey)

	m.mtx.Lock()
	k, ok := m.kv[this.uniKey]
	if ok {
		if !checkMetaVersion(k.meta.meta_version) {
			newMeta := getMetaByTable(this.table)
			if newMeta != nil {
				atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&k.meta)), unsafe.Pointer(newMeta))
			} else {
				//log error
			}
		}
		this.ckey = k
		k.pushCmd(this)
		m.updateLRU(k)
	} else {
		k = newCacheKey(m, this.table, this.key, this.uniKey)
		if nil != k {
			this.ckey = k
			k.pushCmd(this)
			m.updateLRU(k)
			m.kv[this.uniKey] = k
		}
	}
	m.kickCacheKey()

	m.mtx.Unlock()

	if nil != k {
		k.processClientCmd()
	}

}
