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
	"time"
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
	version      int64
	cmdQueue     *cmdQueue //待执行的操作请求
	meta         *dbmeta.TableMeta
	fields       map[string]*proto.Field //字段
	modifyFields map[string]*proto.Field //发生变更尚未更新到sql数据库的字段
	flag         bitfield.BitField32
	slot         *kvSlot
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

	now := time.Now()

	for op := this.cmdQueue.front(); nil != op; {
		if op.isCancel() || op.isTimeout() {
			this.cmdQueue.popFront()
			op.dontReply()
		} else {

		}
	}

	/*	ckey.mtx.Lock()

		if !fromClient {
			//前一条命令执行完毕，解锁队列
			ckey.unlockCmdQueue()
		}

		if ckey.cmdQueueLocked || ckey.cmdQueue.Len() == 0 {
			ckey.mtx.Unlock()
			return
		}

		ctx := &cmdContext{
			commands: []*command{},
		}

		now := time.Now()

		config := conf.GetConfig()

		for ckey.cmdQueue.Len() > 0 {
			e := ckey.cmdQueue.Front()
			cmd := e.Value.(*command)
			if cmd.isClosed() {
				//客户端连接已经关闭
				ckey.cmdQueue.Remove(e)
				cmd.dontReply()
			} else if now.After(cmd.deadline) {
				ckey.cmdQueue.Remove(e)
				//已经超时
				cmd.reply(errcode.ERR_TIMEOUT, nil, -1)
			} else {
				if cmd.cmdType == cmdGet {
					ckey.cmdQueue.Remove(e)
					processGet(ckey, cmd, ctx)
				} else {
					if len(ctx.commands) > 0 {
						//前面已经有get命令了
						break
					} else {
						ckey.cmdQueue.Remove(e)
						switch cmd.cmdType {
						case cmdSet:
							processSet(ckey, cmd, ctx)
						case cmdSetNx:
							processSetNx(ckey, cmd, ctx)
						case cmdCompareAndSet:
							processCompareAndSet(ckey, cmd, ctx)
						case cmdCompareAndSetNx:
							processCompareAndSetNx(ckey, cmd, ctx)
						case cmdIncrBy:
							processIncrBy(ckey, cmd, ctx)
						case cmdDecrBy:
							processDecrBy(ckey, cmd, ctx)
						case cmdDel:
							processDel(ckey, cmd, ctx)
						default:
							//记录日志
						}
						//会产生数据变更的命令只能按序执行
						if len(ctx.commands) > 0 {
							break
						}
					}
				}
			}
		}

		if len(ctx.commands) == 0 {
			ckey.mtx.Unlock()
			return
		}

		if ckey.status == cache_new {
			if !pushSqlLoadReq(ctx, fromClient) {
				ckey.mtx.Unlock()
				if config.ReplyBusyOnQueueFull {
					ctx.reply(errcode.ERR_BUSY, nil, -1)
				} else {
					ctx.dontReply()
				}
				/*
				 * 只有在fromClient==true时pushSqlLoadReq才有可能返回false
				 * 此时必定是由网络层直接调用上来，不会存在排队未处理的cmd,所以不需要调用processCmd
				 * /
				//processCmd(ckey, fromClient)
			} else {
				ckey.lockCmdQueue()
				ckey.mtx.Unlock()
			}
		} else {
			ckey.lockCmdQueue()
			ckey.mtx.Unlock()
			if ctx.getCmdType() == cmdGet {
				ckey.store.issueReadReq(ctx)
			} else {
				ckey.store.issueUpdate(ctx)
			}
		}
	*/
}
