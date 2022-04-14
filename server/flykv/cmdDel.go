package flykv

import (
	"github.com/sniperHW/flyfish/errcode"
	flyproto "github.com/sniperHW/flyfish/proto"
	"github.com/sniperHW/flyfish/proto/cs"
	"time"
)

type cmdDel struct {
	cmdBase
}

func (this *cmdDel) makeResponse(err errcode.Error, fields map[string]*flyproto.Field, version int64) *cs.RespMessage {
	return &cs.RespMessage{
		Seqno: this.seqno,
		Err:   err,
		Data: &flyproto.DelResp{
			Version: version,
		}}
}

func (this *cmdDel) do(proposal *kvProposal) {
	if proposal.kvState == kv_ok {
		proposal.ptype = proposal_snapshot
		proposal.version = -(abs(proposal.version) + 1)
		proposal.cmds = append(proposal.cmds, this)
		proposal.kvState = kv_no_record
	} else {
		this.reply(Err_record_notexist, nil, this.kv.version)
	}
}

func (this *cmdDel) cmdType() flyproto.CmdType {
	return flyproto.CmdType_Del
}

func (s *kvstore) makeDel(kv *kv, deadline time.Time, replyer *replyer, seqno int64, req *flyproto.DelReq) (cmdI, errcode.Error) {

	del := &cmdDel{}

	del.cmdBase.init(del, kv, replyer, seqno, req.Version, deadline, del.makeResponse)

	return del, nil
}
