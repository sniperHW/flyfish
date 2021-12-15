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
}

func (this *cmdKick) makeResponse(err errcode.Error, fields map[string]*flyproto.Field, version int64) *cs.RespMessage {
	return &cs.RespMessage{
		Seqno: this.seqno,
		Err:   err,
		Data:  &flyproto.KickResp{}}
}

func (this *cmdKick) onLoadResult(err error, proposal *kvProposal) {
	return
}

func (this *cmdKick) do(keyvalue *kv, proposal *kvProposal) {
	if keyvalue.kickable() {
		proposal.ptype = proposal_kick
	} else {
		proposal.ptype = proposal_none
		GetSugar().Infof("reply retry")
		this.reply(errcode.New(errcode.Errcode_retry, "please retry again"), nil, 0)
	}
}

func (s *kvstore) makeKick(keyvalue *kv, processDeadline time.Time, respDeadline time.Time, c *net.Socket, seqno int64, req *flyproto.KickReq) (cmdI, errcode.Error) {

	kick := &cmdKick{}

	kick.cmdBase.init(flyproto.CmdType_Kick, c, seqno, nil, processDeadline, respDeadline, &s.wait4ReplyCount, kick.makeResponse)

	return kick, nil
}
