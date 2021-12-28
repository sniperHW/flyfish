package flypd

import (
	"encoding/json"
	"errors"
	"github.com/sniperHW/flyfish/pkg/buffer"
)

const (
	proposalInstallDeployment     = 1
	proposalAddNode               = 2
	proposalRemNode               = 4
	proposalBeginSlotTransfer     = 6
	proposalSlotTransOutOk        = 7
	proposalSlotTransInOk         = 8
	proposalAddSet                = 9
	proposalRemSet                = 10
	proposalSetMarkClear          = 11
	proposalSetMeta               = 12
	proposalUpdateMeta            = 13
	proposalStoreUpdateMetaOk     = 14
	proposalAddLearnerStoreToNode = 15
	proposalFlyKvCommited         = 16
	proposalPromoteLearnerStore   = 17
	proposalRemoveNodeStore       = 18
)

type proposalBase struct {
	reply func(error)
}

type applyable interface {
	apply(pd *pd)
}

type replayable interface {
	replay(pd *pd)
}

func (p proposalBase) Isurgent() bool {
	return true
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
	return buffer.AppendBytes(b, bb)
}

func (p *pd) replayProposal(proposal []byte) error {
	reader := buffer.NewReader(proposal)
	proposalType, err := reader.CheckGetByte()
	if nil != err {
		return err
	}

	var r replayable

	unmarshal := func(rr interface{}) (err error) {
		if err = json.Unmarshal(reader.GetAll(), rr); nil == err {
			r = rr.(replayable)
		}
		return
	}

	switch int(proposalType) {
	case proposalInstallDeployment:
		err = unmarshal(&ProposalInstallDeployment{})
	case proposalAddNode:
		err = unmarshal(&ProposalAddNode{})
	case proposalRemNode:
		err = unmarshal(&ProposalRemNode{})
	case proposalBeginSlotTransfer:
		err = unmarshal(&ProposalBeginSlotTransfer{})
	case proposalSlotTransOutOk:
		err = unmarshal(&ProposalSlotTransOutOk{})
	case proposalSlotTransInOk:
		err = unmarshal(&ProposalSlotTransInOk{})
	case proposalAddSet:
		err = unmarshal(&ProposalAddSet{})
	case proposalRemSet:
		err = unmarshal(&ProposalRemSet{})
	case proposalSetMarkClear:
		err = unmarshal(&ProposalSetMarkClear{})
	case proposalSetMeta:
		err = unmarshal(&ProposalSetMeta{})
	case proposalUpdateMeta:
		err = unmarshal(&ProposalUpdateMeta{})
	case proposalStoreUpdateMetaOk:
		err = unmarshal(&ProposalStoreUpdateMetaOk{})
	case proposalFlyKvCommited:
		err = unmarshal(&ProposalFlyKvCommited{})
	case proposalAddLearnerStoreToNode:
		err = unmarshal(&ProposalAddLearnerStoreToNode{})
	case proposalPromoteLearnerStore:
		err = unmarshal(&ProposalPromoteLearnerStore{})
	case proposalRemoveNodeStore:
		err = unmarshal(&ProposalRemoveNodeStore{})
	default:
		return errors.New("invaild proposal type")
	}

	if nil == err {
		r.replay(p)
	}

	return err
}
