package flykv

import (
	"github.com/sniperHW/flyfish/db"
	"github.com/sniperHW/flyfish/errcode"
	"github.com/sniperHW/flyfish/pkg/buffer"
	"github.com/sniperHW/flyfish/pkg/etcd/raft/raftpb"
	flyproto "github.com/sniperHW/flyfish/proto"
	"time"
)

type kvProposal struct {
	proposalBase
	dbstate db.DBState
	ptype   proposalType
	fields  map[string]*flyproto.Field
	version int64
	cmds    []cmdI
	kv      *kv
}

type kvLinearizableRead struct {
	kv   *kv
	cmds []cmdI
}

func (this *kvProposal) Isurgent() bool {
	return false
}

func (this *kvProposal) OnError(err error) {

	GetSugar().Infof("kvProposal OnError:%v", err)

	for _, v := range this.cmds {
		v.reply(errcode.New(errcode.Errcode_error, err.Error()), nil, 0)
	}

	this.kv.store.mainQueue.AppendHighestPriotiryItem(func() {
		if this.ptype == proposal_kick {
			this.kv.kicking = false
		}
		if this.kv.state == kv_loading {
			this.kv.store.deleteKv(this.kv)
			for f := this.kv.pendingCmd.front(); nil != f; f = this.kv.pendingCmd.front() {
				f.reply(errcode.New(errcode.Errcode_error, err.Error()), nil, 0)
				this.kv.pendingCmd.popFront()
			}
		} else {
			this.kv.processPendingCmd()
		}
	})
}

func (this *kvProposal) Serilize(b []byte) []byte {
	return serilizeKv(b, this.ptype, this.kv.uniKey, this.version, this.fields)
}

func (this *kvProposal) apply() {
	if this.ptype == proposal_kick {
		for _, v := range this.cmds {
			v.reply(nil, nil, 0)
		}

		for f := this.kv.pendingCmd.front(); nil != f; f = this.kv.pendingCmd.front() {
			f.reply(errcode.New(errcode.Errcode_retry, "please try again"), nil, 0)
			this.kv.pendingCmd.popFront()
		}
		this.kv.store.deleteKv(this.kv)
	} else {

		oldState := this.kv.state

		if this.version == 0 {
			this.kv.state = kv_no_record
			this.kv.fields = nil
		} else {
			this.kv.state = kv_ok
		}

		this.kv.version = this.version
		if len(this.fields) > 0 {
			if nil == this.kv.fields {
				this.kv.fields = map[string]*flyproto.Field{}
			}
			for _, v := range this.fields {
				this.kv.fields[v.GetName()] = v
			}
		}

		for _, v := range this.cmds {
			v.reply(nil, this.kv.fields, this.version)
		}

		//update dbUpdateTask
		if this.dbstate != db.DBState_none {
			err := this.kv.updateTask.updateState(this.dbstate, this.version, this.fields)
			if nil != err {
				GetSugar().Errorf("%s updateState error:%v", this.kv.uniKey, err)
			}
		}

		if oldState == kv_loading {
			this.kv.store.lru.update(&this.kv.lru)
		}

		this.kv.processPendingCmd()

	}
}

func (this *kvLinearizableRead) OnError(err error) {

	GetSugar().Errorf("kvLinearizableRead OnError:%v", err)

	for _, v := range this.cmds {
		GetSugar().Infof("reply retry")
		v.reply(errcode.New(errcode.Errcode_retry, "server is busy, please try again!"), nil, 0)
	}

	this.kv.store.mainQueue.AppendHighestPriotiryItem(func() {
		this.kv.processPendingCmd()
	})
}

func (this *kvLinearizableRead) ok() {
	GetSugar().Debugf("kvLinearizableRead ok:%d version:%d", len(this.cmds), this.kv.version)

	for _, v := range this.cmds {
		v.reply(nil, this.kv.fields, this.kv.version)
	}

	this.kv.processPendingCmd()
}

type ProposalConfChange struct {
	confChangeType raftpb.ConfChangeType
	isPromote      bool
	url            string //for add
	nodeID         uint64
	reply          func(error)
}

func (this *ProposalConfChange) GetType() raftpb.ConfChangeType {
	return this.confChangeType
}

func (this *ProposalConfChange) GetURL() string {
	return this.url
}

func (this *ProposalConfChange) GetNodeID() uint64 {
	return this.nodeID
}

func (this *ProposalConfChange) IsPromote() bool {
	return this.isPromote
}

func (this *ProposalConfChange) OnError(err error) {
	this.reply(err)
}

type ProposalUpdateMeta struct {
	proposalBase
	meta  db.DBMeta
	store *kvstore
	reply func()
}

func (this *ProposalUpdateMeta) Isurgent() bool {
	return true
}

func (this *ProposalUpdateMeta) OnError(err error) {
	GetSugar().Errorf("ProposalUpdateMeta error:%v", err)
}

func (this *ProposalUpdateMeta) Serilize(b []byte) []byte {
	return serilizeMeta(this.meta, b)

}

func (this *ProposalUpdateMeta) apply() {
	this.meta.MoveTo(this.store.meta)
	this.reply()
}

type slotTransferType byte

const (
	slotTransferOut = slotTransferType(1)
	slotTransferIn  = slotTransferType(2)
)

type SlotTransferProposal struct {
	proposalBase
	slot         int
	transferType slotTransferType
	store        *kvstore
	reply        func()
	timer        *time.Timer
}

func (this *SlotTransferProposal) Isurgent() bool {
	return true
}

func (this *SlotTransferProposal) OnError(err error) {
	this.store.mainQueue.AppendHighestPriotiryItem(func() {
		delete(this.store.slotsTransferOut, this.slot)
	})
}

func (this *SlotTransferProposal) Serilize(b []byte) []byte {
	b = buffer.AppendByte(b, byte(proposal_slot_transfer))
	b = buffer.AppendByte(b, byte(this.transferType))
	return buffer.AppendInt32(b, int32(this.slot))
}

func (this *SlotTransferProposal) apply() {
	if this.transferType == slotTransferIn {
		this.store.slots.Set(this.slot)
		this.reply()
	} else if this.transferType == slotTransferOut {
		if nil == this.store.slotsKvMap[this.slot] {
			delete(this.store.slotsTransferOut, this.slot)
			this.store.slots.Set(this.slot)
			this.reply()
		} else {
			this.store.processSlotTransferOut(this)
		}
	}
}
