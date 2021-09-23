package flypd

import (
//"encoding/json"
//"errors"
//"fmt"
//"github.com/sniperHW/flyfish/pkg/bitmap"
//"github.com/sniperHW/flyfish/pkg/buffer"
//"github.com/sniperHW/flyfish/pkg/timer"
//sproto "github.com/sniperHW/flyfish/server/proto"
//"github.com/sniperHW/flyfish/server/slot"
//"net"
//"time"
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
	return nil
}
