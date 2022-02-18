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

func (this *cmdKick) do(proposal *kvProposal) {
	if this.kv.kickable() {
		proposal.ptype = proposal_kick
	} else {
		proposal.ptype = proposal_none
		this.kv.markKick = true
		this.kv.updateTask.issueKickDbWriteBack()
		this.kv.waitWriteBackOkKick = this
		this.processDeadline = time.Time{}
		//清空后续消息
		for f := this.kv.pendingCmd.front(); nil != f; f = this.kv.pendingCmd.front() {
			f.reply(errcode.New(errcode.Errcode_retry, "please try again"), nil, 0)
			this.kv.pendingCmd.popFront()
		}
	}
}

func (s *kvstore) makeKick(kv *kv, processDeadline time.Time, respDeadline time.Time, c *net.Socket, seqno int64, req *flyproto.KickReq) (cmdI, errcode.Error) {

	kick := &cmdKick{}

	kick.cmdBase.init(kv, flyproto.CmdType_Kick, c, seqno, nil, processDeadline, respDeadline, &s.wait4ReplyCount, kick.makeResponse)

	return kick, nil
}
