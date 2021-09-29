package flykv

import (
	//"github.com/sniperHW/flyfish/backend/db"
	//"github.com/sniperHW/flyfish/errcode"
	"github.com/sniperHW/flyfish/pkg/buffer"
	//flyproto "github.com/sniperHW/flyfish/proto"
	"github.com/sniperHW/flyfish/pkg/raft"
	"go.etcd.io/etcd/raft/raftpb"
)

type ProposalConfChange struct {
	raft.ProposalConfChangeBase
	reply func()
}

func (this *ProposalConfChange) GetType() raftpb.ConfChangeType {
	return this.ConfChangeType
}

func (this *ProposalConfChange) GetUrl() string {
	return this.Url
}

func (this *ProposalConfChange) GetNodeID() uint64 {
	return this.NodeID
}

func (this *ProposalConfChange) OnError(err error) {

}

type slotTransferType byte

const (
	slotTransferOut = slotTransferType(1)
	slotTransferIn  = slotTransferType(2)
)

type SlotTransferProposal struct {
	slot         int
	transferType slotTransferType
	store        *kvstore
	reply        func()
}

func (this *SlotTransferProposal) Isurgent() bool {
	return true
}

func (this *SlotTransferProposal) OnError(err error) {

}

func (this *SlotTransferProposal) Serilize(b []byte) []byte {
	b = buffer.AppendByte(b, byte(proposal_slot_transfer))
	b = buffer.AppendByte(b, byte(this.transferType))
	return buffer.AppendInt32(b, int32(this.slot))
}

func (this *SlotTransferProposal) OnMergeFinish(b []byte) (ret []byte) {
	if len(b) >= 1024 {
		c := getCompressor()
		cb, err := c.Compress(b)
		if nil != err {
			ret = buffer.AppendByte(b, byte(0))
		} else {
			b = b[:0]
			b = buffer.AppendBytes(b, cb)
			ret = buffer.AppendByte(b, byte(1))
		}
		releaseCompressor(c)
	} else {
		ret = buffer.AppendByte(b, byte(0))
	}
	return
}

func (this *SlotTransferProposal) apply() {
	if this.transferType == slotTransferIn {
		this.store.slots.Set(this.slot)
	} else if this.transferType == slotTransferOut {
		this.store.slots.Clear(this.slot)
	}
	if nil != this.reply {
		this.reply()
	}
}
