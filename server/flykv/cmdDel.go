package flykv

import (
	"github.com/sniperHW/flyfish/backend/db"
	"github.com/sniperHW/flyfish/errcode"
	"github.com/sniperHW/flyfish/pkg/net/cs"
	flyproto "github.com/sniperHW/flyfish/proto"
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

func (this *cmdDel) do(keyvalue *kv, proposal *kvProposal) {
	if keyvalue.state == kv_ok {
		proposal.dbstate = db.DBState_delete
		proposal.version = 0
	} else {
		proposal.ptype = proposal_none
		this.reply(Err_record_notexist, nil, 0)
	}
}

func (s *kvstore) makeDel(keyvalue *kv, processDeadline time.Time, respDeadline time.Time, c *conn, seqno int64, req *flyproto.DelReq) (cmdI, errcode.Error) {

	del := &cmdDel{}

	initCmdBase(&del.cmdBase, flyproto.CmdType_Del, c, seqno, req.Version, processDeadline, respDeadline, &s.wait4ReplyCount, del.makeResponse)

	return del, nil
}
