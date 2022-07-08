package flypd

import (
	"encoding/json"
	"github.com/sniperHW/flyfish/pkg/buffer"
	snet "github.com/sniperHW/flyfish/server/net"
	sproto "github.com/sniperHW/flyfish/server/proto"
)

const (
	proposalInstallDeployment = 1
	proposalAddNode           = 2
	proposalRemNode           = 4
	proposalSlotTransOutOk    = 7
	proposalSlotTransInOk     = 8
	proposalAddSet            = 9
	proposalRemSet            = 10
	proposalSetMarkClear      = 11
	proposalInitMeta          = 12
	proposalFlyKvCommited     = 14
	proposalUpdateMeta        = 17
	proposalNop               = 18
	proposalOrderSequenceID   = 19
)

type proposalBase struct {
	reply func(error)
}

type applyable interface {
	apply(pd *pd)
}

func (p proposalBase) OnError(err error) {
	if nil != p.reply {
		p.reply(err)
	}
}

func (p proposalBase) OnMergeFinish(b []byte) []byte {
	return b
}

func serilizeProposal(b []byte, tt int, p interface{}) []byte {
	b = buffer.AppendByte(b, byte(tt))
	bb, err := json.Marshal(p)
	if nil != err {
		panic(err)
	}
	b = buffer.AppendInt32(b, int32(len(bb)))
	return buffer.AppendBytes(b, bb)
}

type ProposalNop struct {
	proposalBase
}

func (this *ProposalNop) OnError(err error) {
	GetSugar().Errorf("proposalNop error:%v", err)
}

func (this *ProposalNop) Serilize(b []byte) []byte {
	return serilizeProposal(b, proposalNop, this)
}

func (this *ProposalNop) apply(pd *pd) {
	GetSugar().Infof("ProposalNop.apply")
	if !pd.isLeader() {
		return
	}

	if pd.DbMetaMgr.DbMeta.Version == 0 {
		pd.loadInitMeta()
	}

	if pd.Deployment.Version == 0 {
		pd.loadInitDeployment()
	} else {
		pd.slotBalance()
	}
}

type ProposalOrderSequenceID struct {
	proposalBase
	Count   int64
	replyer replyer
	context int64
}

func (this *ProposalOrderSequenceID) OnError(err error) {
	if nil != this.replyer {
		this.replyer.reply(snet.MakeMessage(this.context, &sproto.OrderSequenceIDResp{
			Ok:     false,
			Reason: err.Error(),
		}))
	}
}

func (this *ProposalOrderSequenceID) Serilize(b []byte) []byte {
	return serilizeProposal(b, proposalOrderSequenceID, this)
}

func (this *ProposalOrderSequenceID) apply(pd *pd) {
	//GetSugar().Infof("ProposalOrderSequenceID.apply old:%d new:%d", pd.MaxSequenceID, pd.MaxSequenceID+this.Count)
	pd.MaxSequenceID += this.Count

	if nil != this.replyer {
		this.replyer.reply(snet.MakeMessage(this.context, &sproto.OrderSequenceIDResp{
			Ok:  true,
			Max: pd.MaxSequenceID,
		}))
	}
}
