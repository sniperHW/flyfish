package kvnode

import (
	"sync"
	"time"

	"github.com/sniperHW/flyfish/backend/db"
	"github.com/sniperHW/flyfish/errcode"
	flyproto "github.com/sniperHW/flyfish/proto"
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

const (
	proposal_none     = proposalType(0)
	proposal_snapshot = proposalType(1) //全量数据kv快照,
	proposal_update   = proposalType(2) //fields变更
	proposal_kick     = proposalType(3) //从缓存移除kv
	proposal_lease    = proposalType(4) //数据库update权租约
	proposal_tbmeta   = proposalType(5) //表格元信息
)

type dbUpdateTask struct {
	sync.Mutex
	doing        bool
	updateFields map[string]*flyproto.Field
	version      int64
	dbstate      db.DBState
	keyValue     *kv
}

type dbLoadTask struct {
	keyValue *kv
	cmd      cmdI
}

type kvProposal struct {
	dbstate  db.DBState
	ptype    proposalType
	fields   map[string]*flyproto.Field
	version  int64
	cmds     []cmdI
	keyValue *kv
}

type kvLinearizableRead struct {
	keyValue *kv
	cmds     []cmdI
}

type cmdQueue struct {
	head cmdI
	tail cmdI
}

type kv struct {
	lru           lruElement
	uniKey        string //"table:key"组成的唯一全局唯一键
	key           string
	version       int64
	state         kvState
	kicking       bool
	fields        map[string]*flyproto.Field //字段
	tbmeta        db.TableMeta
	updateTask    dbUpdateTask
	pendingCmd    cmdQueue
	store         *kvstore
	asynTaskCount int
	groupID       int
	snapshot      bool //是否需要快照
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
	}
	return false
}

func (this *kv) kickable() bool {
	if this.store.needWriteBackAll {
		return false
	}

	if this.kicking {
		return false
	}

	if !(this.state == kv_ok || this.state == kv_no_record) {
		return false
	}

	if this.asynTaskCount > 0 || !this.pendingCmd.empty() {
		return false
	}

	if this.updateTask.isDoing() {
		return false
	}

	return true
}

func (this *kv) process(cmd cmdI) {
	if nil != cmd {
		if this.state == kv_new {
			//request load kv from database
			l := &dbLoadTask{
				cmd:      cmd,
				keyValue: this,
			}
			if !this.store.db.issueLoad(l) {
				GetSugar().Infof("reply retry")
				cmd.reply(errcode.New(errcode.Errcode_retry, "server is busy, please try again!"), nil, 0)
				delete(this.store.keyvals[this.groupID].kv, this.uniKey)
			} else {
				this.asynTaskCount++
				this.state = kv_loading
				GetSugar().Debugf("load--------------- %s", this.uniKey)

			}
			return
		} else {
			this.pendingCmd.add(cmd)
			if !this.kicking && (this.state == kv_ok || this.state == kv_no_record) {
				this.store.lru.updateLRU(&this.lru)
			}
		}
	} else {
		this.asynTaskCount--
	}

	if this.pendingCmd.empty() || this.asynTaskCount != 0 {
		return
	} else {
		for {
			cmds := []cmdI{}
			for {
				c := this.pendingCmd.front()
				if nil == c {
					break
				}

				if c.isTimeout() || c.isCancel() {
					this.pendingCmd.popFront()
					c.dontReply()
				} else if !c.check(this) {
					this.pendingCmd.popFront()
				} else {
					if 0 == len(cmds) {
						cmds = append(cmds, c)
						this.pendingCmd.popFront()
					} else {
						f := cmds[0]
						if mergeAbleCmd(f.cmdType()) {
							if f.cmdType() != c.cmdType() {
								//不同类命令，不能合并
								break
							} else if f.cmdType() != flyproto.CmdType_Get && c.checkVersion() {
								//命令要检查版本号，不能跟之前的命令合并
								break
							} else {
								cmds = append(cmds, c)
								this.pendingCmd.popFront()
							}
						} else {
							break
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
				linearizableRead = &kvLinearizableRead{
					keyValue: this,
					cmds:     cmds,
				}
			default:
				proposal = &kvProposal{
					ptype:    proposal_snapshot,
					keyValue: this,
					cmds:     cmds,
					version:  this.version,
					fields:   map[string]*flyproto.Field{},
				}

				for _, v := range cmds {
					v.(interface {
						do(*kv, *kvProposal)
					}).do(this, proposal)
				}

				if proposal.ptype == proposal_none {
					proposal = nil
				}

				//GetSugar().Infof("proposal type %v", proposal.ptype)

			}

			if linearizableRead != nil || proposal != nil {

				var err error

				if nil != linearizableRead {
					err = this.store.rn.IssueLinearizableRead(linearizableRead)
				} else {
					err = this.store.rn.IssueProposal(proposal)
				}

				if nil != err {
					for _, v := range cmds {
						GetSugar().Infof("reply retry")
						v.reply(errcode.New(errcode.Errcode_retry, "server is busy, please try again!"), nil, 0)
					}
				} else {
					this.asynTaskCount++
					return
				}
			}
		}
	}
}
