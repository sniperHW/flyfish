package kvnode

import (
	"github.com/sniperHW/flyfish/dbmeta"
	"github.com/sniperHW/flyfish/proto"
	"github.com/sniperHW/flyfish/util/bitfield"
	"github.com/sniperHW/flyfish/util/ringqueue"
	//"strconv"
	"sync"
	//"sync/atomic"
	//"unsafe"
	//"time"
)

const (
	cache_new     = uint32(1) //正在从数据库加载
	cache_ok      = uint32(2) //
	cache_missing = uint32(3)
	cache_remove  = uint32(4)
)

const (
	sql_none          = uint32(0)
	sql_insert        = uint32(1)
	sql_update        = uint32(2)
	sql_delete        = uint32(3)
	sql_insert_update = uint32(4)
)

const (
	kv_status_offset     = uint32(0)
	mask_kv_status       = uint32(0xF << kv_status_offset) //1-4位kv状态
	kv_sql_flag_offset   = uint32(4)
	mask_kv_sql_flag     = uint32(0xF << kv_sql_flag_offset) //5-8位sql回写标记
	kv_writeback_offset  = uint32(8)
	mask_kv_writeback    = uint32(0xF << kv_writeback_offset) //9-12位当前是否正在执行sql回写
	kv_snapshoted_offset = uint32(12)
	mask_kv_snapshoted   = uint32(0xF << kv_snapshoted_offset) //13-16位是否已经建立过快照
	kv_tmp_offset        = uint32(16)
	mask_kv_tmp          = uint32(0xF << kv_tmp_offset) //17-20位,是否临时kv
	kv_kicking_offset    = uint32(20)
	mask_kv_kicking      = uint32(0xF << kv_kicking_offset) //21-24位,是否正在被踢除
)

type cmdQueue struct {
	queue  *ringqueue.Queue //待执行的操作请求
	locked bool             //队列是否被锁定（前面有op尚未完成）
}

func (this *cmdQueue) empty() bool {
	return this.queue.Front() == nil
}

func (this *cmdQueue) append(op commandI) bool {
	return this.queue.Append(op)
}

func (this *cmdQueue) front() commandI {
	o := this.queue.Front()
	if nil == o {
		return nil
	} else {
		return o.(commandI)
	}
}

func (this *cmdQueue) popFront() commandI {
	o := this.queue.PopFront()
	if nil == o {
		return nil
	} else {
		return o.(commandI)
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
	flag         bitfield.BitField32
	slot         *kvSlot
}

func (this *kv) appendCmd(op commandI) bool {
	this.Lock()
	defer this.Unlock()
	if this.getStatus() == cache_remove {
		return false
	}
	return this.cmdQueue.append(op)
}

//设置remove,清空cmdQueue,向队列内的cmd响应错误码err
func (this *kv) setRemoveAndClearCmdQueue(err int) {
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
		this.slot.removeTmpKv(this)
		return true
	}
}

func (this *kv) getMeta() *dbmeta.TableMeta {
	return (*dbmeta.TableMeta)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&this.meta))))
}

func (this *kv) setSqlFlag(sqlFlag uint32) {
	this.flag.Set(mask_kv_status, kv_sql_flag_offset, sqlFlag)
}

func (this *kv) getSqlFlag() uint32 {
	return this.flag.Get(mask_kv_status, kv_sql_flag_offset)
}

func (this *kv) setStatus(status uint32) {
	this.flag.Set(mask_kv_status, kv_status_offset, status)
}

func (this *kv) getStatus() uint32 {
	return this.flag.Get(mask_kv_status, kv_status_offset)
}

func (this *kv) setTmp(tmp bool) {
	if tmp {
		this.flag.Set(mask_kv_tmp, kv_tmp_offset, uint32(1))
	} else {
		this.flag.Set(mask_kv_tmp, kv_tmp_offset, uint32(0))
	}
}

func (this *kv) isTmp() bool {
	return this.flag.Get(mask_kv_tmp, kv_tmp_offset) == 1
}

func (this *kv) setKicking(kicking bool) {
	if kicking {
		this.flag.Set(mask_kv_tmp, kv_tmp_offset, uint32(1))
	} else {
		this.flag.Set(mask_kv_tmp, kv_tmp_offset, uint32(0))
	}
}

func (this *kv) isKicking() bool {
	return this.flag.Get(mask_kv_kicking, kv_kicking_offset) == 1
}

func (this *kv) setWriteBack(writeback bool) {
	if writeback {
		this.flag.Set(mask_kv_writeback, kv_writeback_offset, uint32(1))
	} else {
		this.flag.Set(mask_kv_writeback, kv_writeback_offset, uint32(0))
	}
}

func (this *kv) isWriteBack() bool {
	return this.flag.Get(mask_kv_writeback, kv_writeback_offset) == 1
}

func (this *kv) setSnapshoted(snapshoted bool) {
	if snapshoted {
		this.flag.Set(mask_kv_snapshoted, kv_snapshoted_offset, uint32(1))
	} else {
		this.flag.Set(mask_kv_snapshoted, kv_snapshoted_offset, uint32(0))
	}
}

func (this *kv) isSnapshoted() bool {
	return this.flag.Get(mask_kv_snapshoted, kv_snapshoted_offset) == 1
}

func newkv(slot *kvSlot, tableMeta *dbmeta.TableMeta, key string, uniKey string, isTmp bool) *kv {

	k := &kv{
		uniKey: uniKey,
		key:    key,
		meta:   tableMeta,
		table:  tableMeta.GetTable(),
		cmdQueue: &cmdQueue{
			queue: ringqueue.New(100),
		},
		modifyFields: map[string]*proto.Field{},
		slot:         slot,
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

	for cmd := this.cmdQueue.front(); nil != cmd; {
		if cmd.isCancel() || cmd.isTimeout() {
			this.cmdQueue.popFront()
			cmd.dontReply()
		} else {
			switch cmd.(type) {
			case *cmdGet:
				this.cmdQueue.popFront()
				asynCmdTaskI = cmd.prepare(asynCmdTaskI)
			case *cmdCompareAndSet, *cmdCompareAndSetNx, *cmdDecr, *cmdDel, *Incr, *Set, *SetNx:
				if nil != asynTask {
					goto loopEnd
				}
				this.cmdQueue.popFront()
				asynCmdTaskI = cmd.prepare(asynCmdTaskI)
				goto loopEnd
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
			this.slot.removeTmpKv(this)
		} else {
			this.Unlock()
		}
		return
	}

	if this.getStatus() == cache_new {
		fullReturn := len(unlockOpQueue) == 0
		if !this.slot.getKvNode().pushSqlLoadReq(asynTask, fullReturn) {
			if this.isTmp() {
				for _, v := range asynTask.getCommands() {
					v.reply(errcode.ERR_BUSY, nil, -1)
				}
				this.setRemoveAndClearCmdQueue(errcode.ERR_BUSY)
				this.Unlock()
				this.slot.removeTmpKv(this)
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
			this.slot.issueReadReq(asynTask)
		default:
			this.slot.issueUpdate(asynTask)
		}
	}
}
