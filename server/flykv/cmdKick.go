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

func (this *cmdKick) makeResponse(err errcode.Error, fields map[string]*flyproto.Field, version int64) *cs.RespMessage {
	return &cs.RespMessage{
		Seqno: this.seqno,
		Err:   err,
		Data:  &flyproto.KickResp{}}
}

func (this *cmdKick) do(proposal *kvProposal) {
	if this.kv.version == this.kv.lastWriteBackVersion {
		proposal.ptype = proposal_kick
	} else {
		this.waitVersion = this.kv.version
		this.kv.updateTask.issueKickDbWriteBack()
		this.kv.pendingCmd.PushFront(this)
	}
	proposal.cmds = append(proposal.cmds, this)
}

func (this *cmdKick) cmdType() flyproto.CmdType {
	return flyproto.CmdType_Kick
}

func (s *kvstore) makeKick(kv *kv, deadline time.Time, replyer *replyer, seqno int64, _ *flyproto.KickReq) (cmdI, errcode.Error) {

	kick := &cmdKick{}

	kick.cmdBase.init(kv, replyer, seqno, nil, deadline, kick.makeResponse)

	return kick, nil
}
