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
			this.processCmd()
		}
	} else {
		if _, ok := this.pendingCmd.tail.(*cmdKick); ok {
			cmd.reply(errcode.New(errcode.Errcode_retry, "server is busy, please try again!"), nil, 0)
		} else {
			this.pendingCmd.pushback(cmd)
		}
	}
}

func (this *kv) processCmd() {
	defer func() {
		if this.pendingCmd.empty() {
			this.store.addKickable(this)
		} else {
			this.store.removeKickable(this)
		}
	}()

	for c := this.pendingCmd.front(); nil != c; c = this.pendingCmd.front() {
		var err errcode.Error
		if !this.store.isLeader() {
			err = Err_not_leader
		} else if c.isTimeout() {
			err = Err_timeout
		} else if !c.versionMatch(this) {
			err = Err_version_mismatch
		}

		if nil != err {
			c.reply(err, nil, 0)
			this.pendingCmd.popFront()
		} else {
			if c.cmdType() == flyproto.CmdType_Get {
				if !this.store.kvnode.config.LinearizableRead {
					//没有开启一致性读，直接返回
					c.reply(nil, this.fields, this.version)
					this.pendingCmd.popFront()
				} else {
					this.store.rn.IssueLinearizableRead(&kvLinearizableRead{
						kv:  this,
						cmd: c,
					})
					break
				}
			} else {
				proposal := &kvProposal{
					ptype:     proposal_snapshot,
					kv:        this,
					cmd:       c,
					version:   this.version,
					fields:    map[string]*flyproto.Field{},
					dbversion: this.lastWriteBackVersion,
				}

				c.(interface {
					do(*kvProposal)
				}).do(proposal)

				if proposal.ptype != proposal_none {
					this.store.rn.IssueProposal(proposal)
					break
				} else {
					if _, ok := proposal.cmd.(*cmdKick); ok {
						break
					} else {
						this.pendingCmd.popFront()
					}
				}
			}
		}
	}
}
