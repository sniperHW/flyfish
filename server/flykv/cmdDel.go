package flykv

import (
	"github.com/sniperHW/flyfish/db"
	"github.com/sniperHW/flyfish/errcode"
	"github.com/sniperHW/flyfish/pkg/net"
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
	if proposal.kvState == kv_no_rok {
		proposal.dbstate = db.DBState_delete
		proposal.version = -(abs(proposal.version) + 1)
	} else {
		this.reply(Err_record_notexist, nil, 0)
	}
}

func (s *kvstore) makeDel(kv *kv, deadline time.Time, c *net.Socket, seqno int64, req *flyproto.DelReq) (cmdI, errcode.Error) {

	del := &cmdDel{}

	del.cmdBase.init(kv.meta, flyproto.CmdType_Del, c, seqno, req.Version, deadline, &s.wait4ReplyCount, del.makeResponse)

	return del, nil
}
