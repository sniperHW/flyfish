package flykv

import (
	"github.com/sniperHW/flyfish/errcode"
	"github.com/sniperHW/flyfish/pkg/net"
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
		proposal.ptype = proposal_none
		this.waitVersion = this.kv.version
		this.kv.updateTask.issueKickDbWriteBack()
	}
}

func (s *kvstore) makeKick(kv *kv, deadline time.Time, c *net.Socket, seqno int64, _ *flyproto.KickReq) (cmdI, errcode.Error) {

	kick := &cmdKick{}

	kick.cmdBase.init(kv, flyproto.CmdType_Kick, c, seqno, nil, deadline, &s.wait4ReplyCount, kick.makeResponse)

	return kick, nil
}
