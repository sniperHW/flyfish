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

func (this *cmdDel) onLoadResult(err error, proposal *kvProposal) {
	if nil == err && nil != this.version && *this.version != proposal.version {
		this.reply(Err_version_mismatch, nil, 0)
	} else if err == db.ERR_RecordNotExist {
		this.reply(Err_record_notexist, nil, 0)
	}
}

func (this *cmdDel) do(proposal *kvProposal) {
	if this.kv.state == kv_ok {
		proposal.dbstate = db.DBState_delete
		proposal.version = 0
	} else {
		proposal.ptype = proposal_none
		this.reply(Err_record_notexist, nil, 0)
	}
}

func (s *kvstore) makeDel(kv *kv, processDeadline time.Time, respDeadline time.Time, c *net.Socket, seqno int64, req *flyproto.DelReq) (cmdI, errcode.Error) {

	del := &cmdDel{}

	del.cmdBase.init(kv, flyproto.CmdType_Del, c, seqno, req.Version, processDeadline, respDeadline, &s.wait4ReplyCount, del.makeResponse)

	return del, nil
}
