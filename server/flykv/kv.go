package flykv

import (
	"container/list"
	"github.com/sniperHW/flyfish/db"
	"github.com/sniperHW/flyfish/errcode"
	flyproto "github.com/sniperHW/flyfish/proto"
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

type cmdQueue struct {
	head cmdI
	tail cmdI
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
	pendingCmd           cmdQueue
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

func (this *cmdQueue) empty() bool {
	return this.head == nil
}

func (this *cmdQueue) pushback(c cmdI) {
	if nil == this.tail {
		this.head = c
	} else {
		this.tail.setNext(c)
	}
	this.tail = c
	c.setNext(nil)
}

func (this *cmdQueue) pushfront(c cmdI) {
	if nil == this.tail {
		this.tail = c
		c.setNext(nil)
	} else {
		c.setNext(this.head)
	}

	this.head = c
}

func (this *cmdQueue) front() cmdI {
	return this.head
}

func (this *cmdQueue) popFront() {
	if !this.empty() {
		f := this.head
		this.head = f.getNext()
		if this.head == nil {
			this.tail = nil
		}
		f.setNext(nil)
	}
}

func (this *kv) pushCmd(cmd cmdI) {
	if this.pendingCmd.empty() {
		this.pendingCmd.pushback(cmd)
		if this.state == kv_new {
			if !this.store.db.issueLoad(&dbLoadTask{
				kv:     this,
				meta:   this.meta,
				uniKey: this.uniKey,
				table:  this.table,
				key:    this.key,
			}) {
				cmd.reply(errcode.New(errcode.Errcode_retry, "server is busy, please try again!"), nil, 0)
				this.store.deleteKv(this)
			} else {
				this.state = kv_loading
			}
		} else {
			this.processCmd(nil)
		}
	} else {
		if _, ok := this.pendingCmd.tail.(*cmdKick); ok {
			cmd.reply(errcode.New(errcode.Errcode_retry, "server is busy, please try again!"), nil, 0)
		} else {
			this.pendingCmd.pushback(cmd)
		}
	}
}

func (this *kv) mergeCmd() (cmds []cmdI) {
	for c := this.pendingCmd.front(); nil != c; c = this.pendingCmd.front() {
		var err errcode.Error
		if !this.store.isLeader() {
			err = Err_not_leader
		} else if c.isTimeout() {
			err = Err_timeout
		}

		if nil != err {
			c.reply(err, nil, 0)
			this.pendingCmd.popFront()
		} else {
			var last cmdI
			if len(cmds) > 0 {
				last = cmds[len(cmds)-1]
			}

			/*
				CmdType_Ping            CmdType = 1
				CmdType_Set             CmdType = 2
				CmdType_Get             CmdType = 3
				CmdType_Del             CmdType = 4
				CmdType_IncrBy          CmdType = 5
				CmdType_DecrBy          CmdType = 6
				CmdType_SetNx           CmdType = 7
				CmdType_CompareAndSet   CmdType = 8
				CmdType_CompareAndSetNx CmdType = 9
				CmdType_Kick            CmdType = 10
				CmdType_ScanNext        CmdType = 11
			*/

			if nil == last {
				cmds = append(cmds, c)
				this.pendingCmd.popFront()
				if !(c.cmdType() == flyproto.CmdType_Get || c.cmdType() == flyproto.CmdType_Get) {

				}
			} else {

			}
		}
	}
}

func (this *kv) processCmd(proposal *kvProposal) {

	for !this.pendingCmd.empty() {

		cmds := []cmdI{}

		for c := this.pendingCmd.front(); nil != c; c = this.pendingCmd.front() {
			var err errcode.Error
			if !this.store.isLeader() {
				err = Err_not_leader
			} else if c.isTimeout() {
				err = Err_timeout
			}

			if nil != err {
				c.reply(err, nil, 0)
				this.pendingCmd.popFront()
			} else {
				var last cmdI
				if len(cmds) > 0 {
					last = cmds[len(cmds)-1]
				}

				//if 0 == len(cmds) {
				//	cmds = append(cmds, c)
				//	this.pendingCmd.popFront()
				//	switch c.cmdType()
				//}

				//if canMerge(cmds, c) {

				//}

				/*if 0 == len(cmds) {
					cmds = append(cmds, c)
					switch c.cmdType() {
					case flyproto.CmdType_Kick:
						break
					case flyproto.CmdType_Del:
						this.pendingCmd.popFront()
						break
					default:
						this.pendingCmd.popFront()
					}
				} else {
					last := cmds[len(cmds)-1]
					if last.cmdType() == flyproto.CmdType_Get && c.cmdType() != flyproto.CmdType_Get {
						//get不能与其它设置类指令合并
						break
					} else {
						cmds = append(cmds, c)
						this.pendingCmd.popFront()
					}
				}*/
			}

			/*if len(cmds) == 0 {
				break
			}

			if this.state != kv_loading && cmds[0].cmdType() == flyproto.CmdType_Get {
				if !this.store.kvnode.config.LinearizableRead {
					//没有开启一致性读，直接返回
					c.reply(nil, this.fields, this.version)
				} else {
					this.store.rn.IssueLinearizableRead(&kvLinearizableRead{
						kv:  this,
						cmd: c,
					})
					this.store.removeKickable(this)
					return
				}
			} else {
				if nil == proposal {
					proposal = &kvProposal{
						kv:        this,
						cmds:      cmds,
						version:   this.version,
						fields:    this.fields,
						dbversion: this.lastWriteBackVersion,
						kvState:   this.state,
					}
				}

				c.(interface {
					do(*kvProposal)
				}).do(proposal)

				if proposal.ptype != proposal_none {
					this.store.rn.IssueProposal(proposal)
					this.store.removeKickable(this)
					return
				} else if cmds[0].cmdType() == flyproto.CmdType_Kick {
					this.store.removeKickable(this)
					return
				}
			}*/
		}
	}

	this.store.addKickable(this)
}
