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

func (this *cmdDel) do(proposal *kvProposal) *kvProposal {
	if proposal.kvState == kv_ok {
		proposal.ptype = proposal_snapshot
		proposal.version = -(abs(proposal.version) + 1)
		proposal.kvState = kv_no_record
		return proposal
	} else {
		this.reply(nil, nil, 0)
		return nil
	}
}

func (this *cmdDel) cmdType() flyproto.CmdType {
	return flyproto.CmdType_Del
}

func (s *kvstore) makeDel(kv *kv, deadline time.Time, replyer *replyer, req *flyproto.DelReq) (cmdI, errcode.Error) {
	del := &cmdDel{}
	del.cmdBase.init(del, kv, replyer, deadline, func(err errcode.Error, _ map[string]*flyproto.Field, _ int64) *cs.RespMessage {
		return &cs.RespMessage{
			Err:  err,
			Data: &flyproto.DelResp{},
		}
	})
	return del, nil
}
