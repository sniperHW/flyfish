package flypd

import (
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
	proposalRemoveNodeStore       = 17
)

type proposalBase struct {
	pd    *pd
	reply func(error)
}

type applyable interface {
	apply()
}

func (p *proposalBase) Isurgent() bool {
	return true
}

func (p *proposalBase) OnError(err error) {
	if nil != p.reply {
		p.reply(err)
	}
}

func (p *proposalBase) OnMergeFinish(b []byte) []byte {
	return b
}

func (p *pd) replayProposal(proposal []byte) error {
	reader := buffer.NewReader(proposal)
	proposalType, err := reader.CheckGetByte()
	if nil != err {
		return err
	}

	switch int(proposalType) {
	case proposalInstallDeployment:
		return p.replayInstallDeployment(&reader)
	case proposalAddNode:
		return p.replayAddNode(&reader)
	case proposalRemNode:
		return p.replayRemNode(&reader)
	case proposalBeginSlotTransfer:
		return p.replayBeginSlotTransfer(&reader)
	case proposalSlotTransOutOk:
		return p.replaySlotTransOutOk(&reader)
	case proposalSlotTransInOk:
		return p.replaySlotTransInOk(&reader)
	case proposalAddSet:
		return p.replayAddSet(&reader)
	case proposalRemSet:
		return p.replayRemSet(&reader)
	case proposalSetMarkClear:
		return p.replaySetMarkClear(&reader)
	case proposalSetMeta:
		return p.replaySetMeta(&reader)
	case proposalUpdateMeta:
		return p.replayUpdateMeta(&reader)
	case proposalStoreUpdateMetaOk:
		return p.replayStoreUpdateMetaOk(&reader)
	default:
		return errors.New("invaild proposal type")
	}

	return nil
}
