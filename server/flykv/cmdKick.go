package flykv

import (
	"github.com/sniperHW/flyfish/errcode"
	flyproto "github.com/sniperHW/flyfish/proto"
	"github.com/sniperHW/flyfish/proto/cs"
	"time"
)

type cmdKick struct {
	cmdBase
	waitVersion int64
}

func (this *cmdKick) do(proposal *kvProposal) *kvProposal {
	if this.kv.version == this.kv.lastWriteBackVersion {
		proposal.ptype = proposal_kick
		return proposal
	} else {
		this.waitVersion = this.kv.version
		this.kv.updateTask.issueKickDbWriteBack()
		this.kv.pendingCmd.PushFront(this.getListElement())
		return nil
	}
}

func (this *cmdKick) cmdType() flyproto.CmdType {
	return flyproto.CmdType_Kick
}

func (s *kvstore) makeKick(kv *kv, deadline time.Time, replyer *replyer, seqno int64, _ *flyproto.KickReq) (cmdI, errcode.Error) {

	kick := &cmdKick{}

	kick.cmdBase.init(kick, kv, replyer, seqno, deadline, func(err errcode.Error, _ map[string]*flyproto.Field, _ int64) *cs.RespMessage {
		return &cs.RespMessage{
			Seqno: seqno,
			Err:   err,
			Data:  &flyproto.KickResp{},
		}
	})

	return kick, nil
}
