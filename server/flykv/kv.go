package flykv

import (
	"container/list"
	"github.com/sniperHW/flyfish/db"
	"github.com/sniperHW/flyfish/errcode"
	flyproto "github.com/sniperHW/flyfish/proto"
	"reflect"
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
}

/*
 *kv.lastWriteBackVersion: db回写完成后，用最后的version提交proposal_last_writeback_version
 *apply的时候将 kv.lastWriteBackVersion设置为proposal_last_writeback_version.version
 *kv.lastWriteBackVersion == kv.version表示db已经跟内存一致
 *新leader当选后，对于所有kv.lastWriteBackVersion != kv.version的kv,都表示无法确定是否与DB一致，执行一次强制回写
 */

type kv struct {
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
	groupID              int
	lastWriteBackVersion int64
	listElement          *list.Element
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

func (this *kv) pushCmd(cmd cmdI) {
	if this.pendingCmd.Len() == 0 {
		this.pendingCmd.PushBack(cmd)
		if this.state == kv_new {
			if !this.store.db.issueLoad(&dbLoadTask{
				kv:     this,
				meta:   this.meta,
				uniKey: this.uniKey,
				table:  this.table,
				key:    this.key,
			}) {
				cmd.reply(errcode.New(errcode.Errcode_retry, "loader is busy, please try later!"), nil, 0)
				this.store.deleteKv(this)
			} else {
				this.state = kv_loading
			}
		} else {
			this.processCmd()
		}
	} else {
		if _, ok := this.pendingCmd.Back().Value.(*cmdKick); ok {
			cmd.reply(errcode.New(errcode.Errcode_retry, "kv is kicking not, please try later!"), nil, 0)
		} else {
			this.pendingCmd.PushBack(cmd)
		}
	}
}

func (this *kv) clearCmds(err errcode.Error) {
	for f := this.pendingCmd.Front(); nil != f; f = this.pendingCmd.Front() {
		this.pendingCmd.Remove(f).(cmdI).reply(err, nil, 0)
	}
}

func canMerge(cmds []cmdI, cmd cmdI) bool {
	if len(cmds) == 0 {
		return true
	} else {
		last := cmds[len(cmds)-1]
		if last.cmdType() != cmd.cmdType() {
			//不同类型命令不允许合并
			return false
		} else if !(cmd.cmdType() == flyproto.CmdType_Get || cmd.cmdType() == flyproto.CmdType_Set) {
			//只有连续的get和set可以合并
			return false
		} else if cmd.cmdType() == flyproto.CmdType_Set && nil != cmd.(*cmdSet).version {
			//带版本号比对的set命令不可合并
			return false
		} else {
			return true
		}
	}
}

func (this *kv) mergeCmd() (cmds []cmdI) {
	for c := this.pendingCmd.Front(); nil != c; c = this.pendingCmd.Front() {
		cmd := c.Value.(cmdI)

		var err errcode.Error
		if cmd.isTimeout() {
			err = Err_timeout
		} else if cmd.cmdType() == flyproto.CmdType_Set && nil != cmd.(*cmdSet).version && *cmd.(*cmdSet).version != this.version {
			err = Err_version_mismatch
		}

		if nil != err {
			this.pendingCmd.Remove(c).(cmdI).reply(err, nil, 0)
		} else {
			if canMerge(cmds, cmd) {
				cmds = append(cmds, cmd)
				this.pendingCmd.Remove(c)
				switch cmd.cmdType() {
				case flyproto.CmdType_Get:
				case flyproto.CmdType_Set:
					if nil != cmd.(*cmdSet).version {
						return
					}
				default:
					return
				}
			} else {
				return
			}
		}
	}
	return
}

func (this *kv) processCmd() {
	if this.store.isLeader() {
		for cmds := this.mergeCmd(); len(cmds) > 0; cmds = this.mergeCmd() {
			if cmds[0].cmdType() == flyproto.CmdType_Get {
				if !this.store.kvnode.config.LinearizableRead {
					//没有开启一致性读，直接返回
					for _, c := range cmds {
						c.reply(nil, this.fields, this.version)
					}
				} else {
					this.store.rn.IssueLinearizableRead(&kvLinearizableRead{
						kv:   this,
						cmds: cmds,
					})
					this.store.removeKickable(this)
					return
				}
			} else {

				proposal := &kvProposal{
					kv:        this,
					version:   this.version,
					dbversion: this.lastWriteBackVersion,
					kvState:   this.state,
				}

				for _, c := range cmds {
					GetSugar().Infof("%s do %s", this.uniKey, reflect.TypeOf(c).String())
					c.(interface{ do(*kvProposal) }).do(proposal)
				}

				if proposal.ptype != proposal_none {
					this.store.rn.IssueProposal(proposal)
					this.store.removeKickable(this)
					return
				} else if cmds[0].cmdType() == flyproto.CmdType_Kick {
					this.store.removeKickable(this)
					return
				}
			}
		}
		this.store.addKickable(this)
	}
}
