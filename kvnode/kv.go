package kvnode

import (
	"container/list"
	"github.com/sniperHW/flyfish/dbmeta"
	"github.com/sniperHW/flyfish/errcode"
	"github.com/sniperHW/flyfish/proto"
	"github.com/sniperHW/flyfish/util/bitfield"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

const (
	cache_invaild = uint32(0)
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

var maxPendingCmdCount int64 = int64(200000) //整个物理节点待处理的命令上限
var maxPendingCmdCountPerKv int = 1000       //单个kv待处理命令上限

func (this *kv) resetStatus() {
	this.modifyFields = map[string]*proto.Field{}
	this.fields = nil
	this.flag = bitfield.NewBitField32(field_status, field_sql_flag, field_writeback, field_snapshoted, field_tmp, field_kicking)
	this.setStatus(cache_new)
}

func (this *kv) getFlagValue(field bitfield.Field32) uint32 {
	v, _ := this.flag.Get(field)
	return v
}

func (this *kv) getMeta() *dbmeta.TableMeta {
	return (*dbmeta.TableMeta)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&this.meta))))
}

func (this *kv) setSqlFlag(sqlFlag uint32) {
	this.flag.Set(field_sql_flag, sqlFlag)
}

func (this *kv) getSqlFlag() uint32 {
	return this.getFlagValue(field_sql_flag)
}

func (this *kv) setStatus(status uint32) {
	this.flag.Set(field_status, status)
}

func (this *kv) getStatus() uint32 {
	return this.getFlagValue(field_status)
}

func (this *kv) setKicking(kicking bool) {
	if kicking {
		this.flag.Set(field_kicking, uint32(1))
	} else {
		this.flag.Set(field_kicking, uint32(0))
	}
}

func (this *kv) isKicking() bool {
	return this.getFlagValue(field_kicking) == uint32(1)
}

func (this *kv) setWriteBack(writeback bool) {
	if writeback {
		this.flag.Set(field_writeback, uint32(1))
	} else {
		this.flag.Set(field_writeback, uint32(0))
	}
}

func (this *kv) isWriteBack() bool {
	return this.getFlagValue(field_writeback) == uint32(1)
}

func (this *kv) setSnapshoted(snapshoted bool) {
	if snapshoted {
		this.flag.Set(field_snapshoted, uint32(1))
	} else {
		this.flag.Set(field_snapshoted, uint32(0))
	}
}

func (this *kv) isSnapshoted() bool {
	return this.getFlagValue(field_snapshoted) == uint32(1)
}

func (this *kv) kickable() bool {

	status := this.getStatus()

	if status == cache_remove {
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

	logger.Debugln("set ok", this.uniKey, this.fields)

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
	return k
}

func (this *kv) processCmd(op commandI) {

	var asynTask asynCmdTaskI

	callKick := false
	removeKv := false
	issueUpdate := false
	issueReadReq := false

	this.Lock()

	defer func() {
		this.Unlock()
		if removeKv {
			asynTask.reply(errcode.ERR_OK)
			this.store.removeKv(this, true)
		} else if callKick {
			if !this.store.kick(asynTask.(*asynCmdTaskKick)) {
				asynTask.reply(errcode.ERR_RETRY)
				this.processCmd(nil)
			}
		} else if issueUpdate {
			this.store.issueUpdate(asynTask)
		} else if issueReadReq {
			this.store.issueReadReq(asynTask)
		}
	}()

	if nil != op {

		if this.getStatus() == cache_remove {
			logger.Debugln(this.uniKey, "cache_remove", "retry")
			op.reply(errcode.ERR_RETRY, nil, 0)
			return
		}

		if atomic.LoadInt64(&wait4ReplyCount) > 500000 {
			op.reply(errcode.ERR_RETRY, nil, 0)
			return
		}

		if this.cmdQueue.queue.Len() > maxPendingCmdCountPerKv {
			op.reply(errcode.ERR_RETRY, nil, 0)
			return
		}

		this.cmdQueue.append(op)

	} else {
		this.cmdQueue.unlock()
	}

	if this.cmdQueue.isLocked() || this.cmdQueue.empty() {
		return
	}

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
		return
	}

	if this.getStatus() == cache_new {
		/*
		 *   op != nil表示调用直接来自网络连接
		 *   此时如果load队列满会直接返回false,向客户端返回retry
		 */
		if !this.store.getKvNode().sqlMgr.pushLoadReq(asynTask, op != nil) {
			for _, v := range asynTask.getCommands() {
				v.reply(errcode.ERR_RETRY, nil, -1)
			}
		} else {
			this.cmdQueue.lock()
		}
	} else {
		this.cmdQueue.lock()
		switch asynTask.(type) {
		case *asynCmdTaskGet:
			issueReadReq = true
		case *asynCmdTaskKick:
			if this.getStatus() == cache_new {
				removeKv = true
				this.setStatus(cache_remove)
			} else {
				callKick = true
			}
		default:
			issueUpdate = true
		}
	}

}
