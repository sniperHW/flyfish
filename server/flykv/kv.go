package flykv

import (
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
	cmd    cmdI
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
	lru                  lruElement
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
	asynTaskCount        int
	groupID              int
	lastWriteBackVersion int64
	markKick             bool
	waitWriteBackOkKick  cmdI
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

func (this *cmdQueue) add(c cmdI) {
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

func mergeAbleCmd(cmdType flyproto.CmdType) bool {
	switch cmdType {
	case flyproto.CmdType_Get, flyproto.CmdType_Set, flyproto.CmdType_IncrBy, flyproto.CmdType_DecrBy:
		return true
	default:
		return false
	}
}

func (this *kv) kickable() bool {
	if this.state < kv_ok {
		return false
	} else if this.asynTaskCount > 0 {
		return false
	} else if this.version != this.lastWriteBackVersion {
		return false
	} else {
		return true
	}
}

func (this *kv) kick() {
	this.asynTaskCount++
	this.store.rn.IssueProposal(&kvProposal{
		ptype:     proposal_kick,
		kv:        this,
		version:   this.version,
		dbversion: this.lastWriteBackVersion,
	})
}

func (this *kv) pushCmd(cmd cmdI) {
	this.processCmd(cmd)
}

func (this *kv) processPendingCmd() {
	if !this.store.isLeader() {
		this.asynTaskCount--
		for c := this.pendingCmd.front(); nil != c; c = this.pendingCmd.front() {
			this.pendingCmd.popFront()
			c.reply(errcode.New(errcode.Errcode_not_leader), nil, 0)
		}
	} else if this.markKick && this.lastWriteBackVersion == this.version {
		this.asynTaskCount--
		this.kick()
	} else {
		this.processCmd(nil)
	}
}

func (this *kv) processCmd(cmd cmdI) {
	if nil != cmd {
		if this.state == kv_new {
			//request load kv from database
			l := &dbLoadTask{
				cmd:    cmd,
				kv:     this,
				meta:   this.meta,
				uniKey: this.uniKey,
				table:  this.table,
				key:    this.key,
			}
			if !this.store.db.issueLoad(l) {
				cmd.reply(errcode.New(errcode.Errcode_retry, "server is busy, please try again!"), nil, 0)
				this.store.deleteKv(this)
			} else {
				this.asynTaskCount++
				this.state = kv_loading
			}
			return
		} else {
			this.pendingCmd.add(cmd)
		}
	} else {
		this.asynTaskCount--
	}

	if this.pendingCmd.empty() || this.asynTaskCount != 0 {
		return
	}

	for {
		cmds := []cmdI{}

		for c := this.pendingCmd.front(); nil != c; c = this.pendingCmd.front() {
			if c.isTimeout() {
				c.dropReply()
			} else if !c.versionMatch(this) {
				this.pendingCmd.popFront()
				c.reply(Err_version_mismatch, nil, 0)
			} else {
				if 0 == len(cmds) {
					cmds = append(cmds, c)
					this.pendingCmd.popFront()
				} else {
					f := cmds[0]
					if !mergeAbleCmd(f.cmdType()) || f.cmdType() != c.cmdType() {
						break
					} else {
						cmds = append(cmds, c)
						this.pendingCmd.popFront()
					}
				}
			}
		}

		if len(cmds) == 0 {
			return
		}

		var linearizableRead *kvLinearizableRead
		var proposal *kvProposal

		switch cmds[0].cmdType() {
		case flyproto.CmdType_Get:
			if !this.store.kvnode.config.LinearizableRead {
				//没有开启一致性读，直接返回
				for _, v := range cmds {
					v.reply(nil, this.fields, this.version)
				}
			} else {
				linearizableRead = &kvLinearizableRead{
					kv:   this,
					cmds: cmds,
				}
			}
		default:
			proposal = &kvProposal{
				ptype:     proposal_snapshot,
				kv:        this,
				cmds:      cmds,
				version:   this.version,
				fields:    map[string]*flyproto.Field{},
				dbversion: this.lastWriteBackVersion,
			}

			for _, v := range cmds {
				v.(interface {
					do(*kvProposal)
				}).do(proposal)
			}

			if proposal.ptype == proposal_none {
				proposal = nil
			}
		}

		if nil != linearizableRead {
			this.store.rn.IssueLinearizableRead(linearizableRead)
			this.asynTaskCount++
			return
		} else if nil != proposal {
			this.store.rn.IssueProposal(proposal)
			this.asynTaskCount++
			return
		}
	}
}
