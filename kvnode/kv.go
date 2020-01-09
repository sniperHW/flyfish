package kvnode

import (
	"container/list"
	"github.com/sniperHW/flyfish/dbmeta"
	"github.com/sniperHW/flyfish/errcode"
	"github.com/sniperHW/flyfish/proto"
	"github.com/sniperHW/flyfish/util/bitfield"
	//"github.com/sniperHW/kendynet/util"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

const (
	cache_new     = uint32(1) //正在从数据库加载
	cache_ok      = uint32(2) //
	cache_missing = uint32(3)
	cache_remove  = uint32(4)
)

const (
	sql_none          = uint32(0)
	sql_insert_update = uint32(1)
	sql_update        = uint32(2)
	sql_delete        = uint32(3)
)

var (
	field_status     = bitfield.MakeFiled32(0xF)
	field_sql_flag   = bitfield.MakeFiled32(0xF0)
	field_writeback  = bitfield.MakeFiled32(0xF00)
	field_snapshoted = bitfield.MakeFiled32(0xF000)
	field_tmp        = bitfield.MakeFiled32(0xF0000)
	field_kicking    = bitfield.MakeFiled32(0xF00000)
)

func getDeadline(timeout uint32) (time.Time, time.Time) {
	now := time.Now()
	t := time.Duration(timeout) * time.Millisecond
	processDeadline := now.Add(t / 2)
	respDeadline := now.Add(t)
	return processDeadline, respDeadline
}

type cmdQueue struct {
	queue  *list.List
	locked bool //队列是否被锁定（前面有op尚未完成）
}

func (this *cmdQueue) len() int {
	return this.queue.Len()
}

func (this *cmdQueue) empty() bool {
	return this.queue.Len() == 0
}

func (this *cmdQueue) append(op commandI) {
	this.queue.PushBack(op)
}

func (this *cmdQueue) front() commandI {
	if this.empty() {
		return nil
	} else {
		return this.queue.Front().Value.(commandI)
	}
}

func (this *cmdQueue) popFront() commandI {
	if this.empty() {
		return nil
	} else {
		e := this.queue.Front()
		cmd := e.Value.(commandI)
		this.queue.Remove(e)
		return cmd
	}
}

func (this *cmdQueue) lock() {
	this.locked = true
}

func (this *cmdQueue) unlock() {
	this.locked = false
}

func (this *cmdQueue) isLocked() bool {
	return this.locked
}

type kv struct {
	sync.Mutex
	uniKey       string
	key          string
	table        string
	version      int64
	cmdQueue     *cmdQueue //待执行的操作请求
	meta         *dbmeta.TableMeta
	fields       map[string]*proto.Field //字段
	modifyFields map[string]*proto.Field //发生变更尚未更新到sql数据库的字段
	flag         *bitfield.BitField32
	store        *kvstore
	nnext        *kv
	pprev        *kv
}

var maxPendingCmdCount int64 = int64(500000) //整个物理节点待处理的命令上限
var maxPendingCmdCountPerKv int = 1000       //单个kv待处理命令上限

func (this *kv) appendCmd(op commandI) int32 {

	if atomic.LoadInt64(&wait4ReplyCount) > 500000 {
		return errcode.ERR_BUSY
	}

	this.Lock()
	defer this.Unlock()
	if this.getStatus() == cache_remove {
		Debugln(this.uniKey, "cache_remove", "retry")
		return errcode.ERR_RETRY
	}

	if this.isKicking() {
		Debugln(this.uniKey, "kicking", "retry")
		return errcode.ERR_RETRY
	}

	if this.cmdQueue.queue.Len() > maxPendingCmdCountPerKv {
		return errcode.ERR_BUSY
	}

	this.cmdQueue.append(op)
	return errcode.ERR_OK
}

//设置remove,清空cmdQueue,向队列内的cmd响应错误码err
func (this *kv) setRemoveAndClearCmdQueue(err int32) {
	//Debugln("setRemoveAndClearCmdQueue", this.uniKey, err, util.CallStack(100))
	this.setStatus(cache_remove)
	for cmd := this.cmdQueue.popFront(); nil != cmd; {
		cmd.reply(err, nil, 0)
	}
}

func (this *kv) tryRemoveTmp(err int32) bool {
	this.Lock()
	isTmp := this.isTmp()
	if isTmp {
		this.Unlock()
		return false
	} else {
		this.setRemoveAndClearCmdQueue(err)
		this.Unlock()
		this.store.removeTmpKv(this)
		return true
	}
}

func (this *kv) getMeta() *dbmeta.TableMeta {
	return (*dbmeta.TableMeta)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&this.meta))))
}

func (this *kv) setSqlFlag(sqlFlag uint32) {
	this.flag.Set(field_sql_flag, sqlFlag)
}

func (this *kv) getSqlFlag() uint32 {
	v, _ := this.flag.Get(field_sql_flag)
	return v
}

func (this *kv) setStatus(status uint32) {
	this.flag.Set(field_status, status)
}

func (this *kv) getStatus() uint32 {
	status, _ := this.flag.Get(field_status)
	return status
}

func (this *kv) setTmp(tmp bool) {
	if tmp {
		this.flag.Set(field_tmp, uint32(1))
	} else {
		this.flag.Set(field_tmp, uint32(0))
	}
}

func (this *kv) isTmp() bool {
	v, _ := this.flag.Get(field_tmp)
	return v == uint32(1)
}

func (this *kv) setKicking(kicking bool) {
	if kicking {
		this.flag.Set(field_kicking, uint32(1))
	} else {
		this.flag.Set(field_kicking, uint32(0))
	}
}

func (this *kv) isKicking() bool {
	v, _ := this.flag.Get(field_kicking)
	return v == uint32(1)
}

func (this *kv) setWriteBack(writeback bool) {
	if writeback {
		this.flag.Set(field_writeback, uint32(1))
	} else {
		this.flag.Set(field_writeback, uint32(0))
	}
}

func (this *kv) isWriteBack() bool {
	v, _ := this.flag.Get(field_writeback)
	return v == uint32(1)
}

func (this *kv) setSnapshoted(snapshoted bool) {
	if snapshoted {
		this.flag.Set(field_snapshoted, uint32(1))
	} else {
		this.flag.Set(field_snapshoted, uint32(0))
	}
}

func (this *kv) isSnapshoted() bool {
	v, _ := this.flag.Get(field_snapshoted)
	return v == uint32(1)
}

func (this *kv) kickable() bool {

	status := this.getStatus()

	if !(status == cache_ok || status == cache_missing) {
		return false
	}

	if this.cmdQueue.isLocked() {
		return false
	}

	if !this.cmdQueue.empty() {
		return false
	}

	if this.isWriteBack() {
		return false
	}

	return true
}

func (this *kv) setMissing() {
	this.version = 0
	this.setStatus(cache_missing)
	this.fields = nil
	this.modifyFields = map[string]*proto.Field{}
}

func (this *kv) setOK(version int64, fields map[string]*proto.Field) {
	this.version = version
	this.setStatus(cache_ok)
	modify := false

	if nil == this.fields {
		this.fields = map[string]*proto.Field{}
	} else {
		modify = true
	}

	for k, v := range fields {
		if !(k == "__version__" || k == "__key__") {
			this.fields[k] = v
			if modify {
				this.modifyFields[k] = v
			}
		}
	}

	Debugln("set ok", this.uniKey, this.fields)

}

func newkv(store *kvstore, tableMeta *dbmeta.TableMeta, key string, uniKey string, isTmp bool) *kv {
	g_metric.addNewKVCount()
	k := &kv{
		uniKey: uniKey,
		key:    key,
		meta:   tableMeta,
		table:  tableMeta.GetTable(),
		cmdQueue: &cmdQueue{
			queue: list.New(),
		},
		modifyFields: map[string]*proto.Field{},
		store:        store,
		flag:         bitfield.NewBitField32(field_status, field_sql_flag, field_writeback, field_snapshoted, field_tmp, field_kicking),
	}

	k.setStatus(cache_new)
	k.setTmp(isTmp)

	return k
}

func (this *kv) processQueueCmd(unlockOpQueue ...bool) {

	this.Lock()
	if len(unlockOpQueue) > 0 {
		this.cmdQueue.unlock()
	}

	if this.cmdQueue.isLocked() || this.cmdQueue.empty() {
		this.Unlock()
		return
	}

	var asynTask asynCmdTaskI
	flagPop := false

	cmd := this.cmdQueue.front()
	for ; nil != cmd; cmd = this.cmdQueue.front() {
		if cmd.isCancel() || cmd.isTimeout() {
			this.cmdQueue.popFront()
			cmd.dontReply()
		} else {
			switch cmd.(type) {
			case *cmdGet, *cmdCompareAndSet, *cmdCompareAndSetNx, *cmdIncrDecr, *cmdDel, *cmdSet, *cmdSetNx, *cmdKick:
				asynTask, flagPop = cmd.prepare(asynTask)

				if flagPop {
					this.cmdQueue.popFront()
				}

				if nil != asynTask && !flagPop {
					//后面的命令无法再合并
					goto loopEnd
				}

			default:
				this.cmdQueue.popFront()
				//记录日志
			}
		}
	}

loopEnd:

	if nil == asynTask {
		if this.isTmp() {
			this.setRemoveAndClearCmdQueue(errcode.ERR_BUSY)
			this.Unlock()
			this.store.removeTmpKv(this)
		} else {
			this.Unlock()
		}
		return
	}

	if this.getStatus() == cache_new {
		fullReturn := len(unlockOpQueue) == 0
		if !this.store.getKvNode().sqlMgr.pushLoadReq(asynTask, fullReturn) {
			if this.isTmp() {
				for _, v := range asynTask.getCommands() {
					v.reply(errcode.ERR_BUSY, nil, -1)
				}
				this.setRemoveAndClearCmdQueue(errcode.ERR_BUSY)
				this.Unlock()
				this.store.removeTmpKv(this)
			}
		} else {
			this.cmdQueue.lock()
			this.Unlock()
		}
	} else {
		this.cmdQueue.lock()
		this.Unlock()
		switch asynTask.(type) {
		case *asynCmdTaskGet:
			this.store.issueReadReq(asynTask)
		case *asynCmdTaskKick:
			if !this.store.kick(asynTask.(*asynCmdTaskKick)) {
				asynTask.reply(errcode.ERR_OTHER)
				this.processQueueCmd(true)
			}
		default:
			this.store.issueUpdate(asynTask)
		}
	}
}
