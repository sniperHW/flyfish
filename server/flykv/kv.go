package flykv

import (
	"github.com/sniperHW/flyfish/db"
	"github.com/sniperHW/flyfish/errcode"
	"github.com/sniperHW/flyfish/pkg/list"
	flyproto "github.com/sniperHW/flyfish/proto"
	//"reflect"
	"sync"
)

type kvState byte
type proposalType uint8

const (
	kv_invaild   = kvState(0)
	kv_new       = kvState(1)
	kv_loading   = kvState(2)
	kv_ok        = kvState(3)
	kv_no_record = kvState(4)
)

type dbUpdateTask struct {
	sync.Mutex
	doing bool
	kv    *kv
	state db.UpdateState
}

type dbLoadTask struct {
	kv     *kv
	meta   db.TableMeta
	table  string
	uniKey string //"table:key"组成的唯一全局唯一键
	key    string
	term   int64
}

/*
 *kv.lastWriteBackVersion: db回写完成后，用最后的version提交proposal_last_writeback_version
 *apply的时候将 kv.lastWriteBackVersion设置为proposal_last_writeback_version.version
 *kv.lastWriteBackVersion == kv.version表示db已经跟内存一致
 *新leader当选后，对于所有kv.lastWriteBackVersion != kv.version的kv,都表示无法确定是否与DB一致，执行一次强制回写
 */

type kv struct {
	listElement          list.Element
	slot                 int
	table                string
	uniKey               string //"table:key"组成的唯一全局唯一键
	key                  string
	version              int64
	state                kvState
	fields               map[string]*flyproto.Field //字段
	meta                 db.TableMeta
	updateTask           dbUpdateTask
	pendingCmd           *list.List
	store                *kvstore
	lastWriteBackVersion int64
}

func abs(v int64) int64 {
	if v > 0 {
		return v
	} else {
		return 0 - v
	}
}

func (this *kv) getField(key string) (v *flyproto.Field) {
	if this.state == kv_ok {
		if v = this.fields[key]; nil == v {
			v = flyproto.PackField(key, this.meta.GetDefaultValue(key))
		}
	}
	return
}

func (this *kv) kickable() bool {
	return this.listElement.List() != nil
}

func (this *kv) pushCmd(cmd cmdI) {
	last := this.pendingCmd.Back()
	this.pendingCmd.PushBack(cmd.getListElement())
	if this.state == kv_new {
		if !this.store.db.issueLoad(&dbLoadTask{
			kv:     this,
			meta:   this.meta,
			uniKey: this.uniKey,
			table:  this.table,
			key:    this.key,
			term:   this.store.getTerm(),
		}) {
			cmd.reply(errcode.New(errcode.Errcode_retry, "loader is busy, please try later!"), nil, 0)
			this.store.deleteKv(this)
		} else {
			this.state = kv_loading
		}
	} else if this.kickable() {
		if nil != last {
			GetSugar().Errorf("should not go here")
		}
		this.processCmd()
	}
}

func (this *kv) clearCmds(err errcode.Error) {
	for f := this.pendingCmd.Front(); nil != f; f = this.pendingCmd.Front() {
		this.pendingCmd.Remove(f).(cmdI).reply(err, nil, this.version)
	}
}

func (this *kv) popCmd() (ret cmdI) {
	var batch *batchCmd
	for c := this.pendingCmd.Front(); nil != c; c = this.pendingCmd.Front() {
		cmd := this.pendingCmd.Remove(c).(cmdI)
		if cmd.isTimeout() {
			cmd.reply(Err_timeout, nil, 0)
		} else if cmd.checkVersion() {
			if nil != ret {
				this.pendingCmd.PushFront(cmd.getListElement())
			} else {
				ret = cmd
			}
			return
		} else {
			switch cmd.cmdType() {
			case flyproto.CmdType_Get, flyproto.CmdType_Set, flyproto.CmdType_IncrBy:
				if nil == batch {
					batch = &batchCmd{cmdtype: cmd.cmdType()}
					batch.addCmd(cmd)
					ret = batch
				} else if batch.cmdType() == cmd.cmdType() {
					batch.addCmd(cmd)
				} else {
					this.pendingCmd.PushFront(cmd.getListElement())
					return
				}
			default:
				if nil != ret {
					this.pendingCmd.PushFront(cmd.getListElement())
				} else {
					ret = cmd
				}
				return
			}
		}
	}
	return
}

func (this *kv) processCmd() {
	if this.store.isLeader() {
		for cmd := this.popCmd(); nil != cmd; cmd = this.popCmd() {
			if cmd.cmdType() == flyproto.CmdType_Get {
				if !this.store.kvnode.config.LinearizableRead {
					//没有开启一致性读，直接返回
					cmd.reply(nil, this.fields, this.version)
				} else {
					this.store.rn.IssueLinearizableRead(&kvLinearizableRead{
						kv:  this,
						cmd: cmd,
					})
					this.store.removeKickable(this)
					return
				}
			} else {
				if proposal := cmd.(interface{ do(*kvProposal) *kvProposal }).do(&kvProposal{
					kv:        this,
					version:   this.version,
					dbversion: this.lastWriteBackVersion,
					kvState:   this.state,
					cmd:       cmd,
				}); nil != proposal {
					this.store.rn.IssueProposal(proposal)
					this.store.removeKickable(this)
					return
				} else if cmd.cmdType() == flyproto.CmdType_Kick {
					//kick需要等待回写完成再执行
					this.store.removeKickable(this)
					return
				}
			}
		}
		this.store.addKickable(this)
	}
}
