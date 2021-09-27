package flypd

import (
	//"encoding/json"
	"errors"
	//"fmt"
	//"github.com/sniperHW/flyfish/pkg/bitmap"
	"github.com/sniperHW/flyfish/pkg/buffer"
	//"github.com/sniperHW/flyfish/pkg/timer"
	//sproto "github.com/sniperHW/flyfish/server/proto"
	//"github.com/sniperHW/flyfish/server/slot"
	//"net"
	//"time"
)

const (
	proposalInstallDeployment      = 1
	proposalAddNode                = 2
	proposalNotifyAddNodeResp      = 3
	proposalRemNode                = 4
	proposalNotifyRemNodeResp      = 5
	proposalBeginSlotTransfer      = 6
	proposalNotifySlotTransOutResp = 7
	proposalNotifySlotTransInResp  = 8
	proposalAddSet                 = 9
	proposalRemSet                 = 10
)

type proposalBase struct {
	pd    *pd
	reply func(...error)
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
	case proposalNotifyAddNodeResp:
		return p.replayNotifyAddNodeResp(&reader)
	case proposalRemNode:
		return p.replayRemNode(&reader)
	case proposalNotifyRemNodeResp:
		return p.replayNotifyRemNodeResp(&reader)
	case proposalBeginSlotTransfer:
		return p.replayBeginSlotTransfer(&reader)
	case proposalNotifySlotTransOutResp:
		return p.replayNotifySlotTransOutResp(&reader)
	case proposalNotifySlotTransInResp:
		return p.replayNotifySlotTransInResp(&reader)
	case proposalAddSet:
		return p.replayAddSet(&reader)
	case proposalRemSet:
		return p.replayRemSet(&reader)
	default:
		return errors.New("invaild proposal type")
	}

	return nil
}
