package flykv

import (
	"github.com/sniperHW/flyfish/db"
	"github.com/sniperHW/flyfish/errcode"
	flyproto "github.com/sniperHW/flyfish/proto"
	"sync"
	"time"
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
	doing        bool
	updateFields map[string]*flyproto.Field
	version      int64
	dbstate      db.DBState
	kv           *kv
}

type dbLoadTask struct {
	kv  *kv
	cmd cmdI
}

type cmdQueue struct {
	head cmdI
	tail cmdI
}

type kv struct {
	slot          int
	lru           lruElement
	table         string
	uniKey        string //"table:key"组成的唯一全局唯一键
	key           string
	version       int64
	state         kvState
	kicking       bool
	fields        map[string]*flyproto.Field //字段
	meta          db.TableMeta
	updateTask    dbUpdateTask
	pendingCmd    cmdQueue
	store         *kvstore
	asynTaskCount int
	groupID       int
}

/*
 * 0表示key不存在，因此inc时要越过0
 */
func incVersion(version int64) int64 {
	version += 1
	if 0 == version {
		return 1
	} else {
		return version
	}
}

func genVersion() int64 {
	return time.Now().UnixNano()
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
	if this.store.needWriteBackAll {
		return false
	} else if this.kicking {
		return false
	} else if !(this.state == kv_ok || this.state == kv_no_record) {
		return false
	} else if this.asynTaskCount > 0 || !this.pendingCmd.empty() {
		return false
	} else if this.updateTask.isDoing() {
		return false
	} else {
		return true
	}
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
	} else {
		this.processCmd(nil)
	}
}

func (this *kv) processCmd(cmd cmdI) {
	if nil != cmd {
		if this.state == kv_new {
			//request load kv from database
			l := &dbLoadTask{
				cmd: cmd,
				kv:  this,
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
			if this.state == kv_ok || this.state == kv_no_record {
				this.store.lru.update(&this.lru)
			}
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
				ptype:   proposal_snapshot,
				kv:      this,
				cmds:    cmds,
				version: this.version,
				fields:  map[string]*flyproto.Field{},
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
			if proposal.ptype == proposal_kick {
				this.kicking = true
			}
			this.asynTaskCount++
			return
		}
	}
}
