package flykv

import (
	"github.com/sniperHW/flyfish/db"
	"github.com/sniperHW/flyfish/pkg/buffer"
	"github.com/sniperHW/flyfish/pkg/etcd/raft/raftpb"
	"time"
)

type ProposalConfChange struct {
	confChangeType raftpb.ConfChangeType
	isPromote      bool
	url            string //for add
	nodeID         uint64
	reply          func()
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
	GetSugar().Errorf("ProposalConfChange error:%v", err)
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
