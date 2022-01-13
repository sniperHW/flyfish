package flykv

import (
	"github.com/sniperHW/flyfish/db"
	"github.com/sniperHW/flyfish/errcode"
	"github.com/sniperHW/flyfish/pkg/net"
	flyproto "github.com/sniperHW/flyfish/proto"
	"github.com/sniperHW/flyfish/proto/cs"
	"time"
)

type cmdSet struct {
	cmdBase
	fields map[string]*flyproto.Field
}

func (this *cmdSet) makeResponse(err errcode.Error, fields map[string]*flyproto.Field, version int64) *cs.RespMessage {
	return &cs.RespMessage{
		Seqno: this.seqno,
		Err:   err,
		Data: &flyproto.SetResp{
			Version: version,
		}}
}

func (this *cmdSet) onLoadResult(err error, proposal *kvProposal) {
	if nil == err && nil != this.version && *this.version != proposal.version {
		this.reply(Err_version_mismatch, nil, 0)
	} else {
		if err == db.ERR_RecordNotExist {
			//记录不存在，为记录生成版本号
			proposal.version = genVersion()
			//对于不在set中field,使用defalutValue填充
			this.kv.meta.FillDefaultValues(this.fields)
			proposal.fields = this.fields
			proposal.dbstate = db.DBState_insert
		} else {
			proposal.version = incVersion(proposal.version)
			for k, v := range this.fields {
				proposal.fields[k] = v
			}
			proposal.dbstate = db.DBState_update
		}
	}
}

func (this *cmdSet) do(proposal *kvProposal) {
	if this.kv.state == kv_no_record {
		proposal.version = genVersion()
		proposal.dbstate = db.DBState_insert
		this.kv.meta.FillDefaultValues(this.fields)
		proposal.fields = this.fields
	} else {
		proposal.ptype = proposal_update
		proposal.version = incVersion(proposal.version)
		proposal.fields = this.fields
		proposal.dbstate = db.DBState_update
	}
}

func (s *kvstore) makeSet(kv *kv, processDeadline time.Time, respDeadline time.Time, c *net.Socket, seqno int64, req *flyproto.SetReq) (cmdI, errcode.Error) {
	if len(req.GetFields()) == 0 {
		return nil, errcode.New(errcode.Errcode_error, "set fields is empty")
	}

	if err := kv.meta.CheckFields(req.GetFields()...); nil != err {
		return nil, errcode.New(errcode.Errcode_error, err.Error())
	}

	set := &cmdSet{
		fields: map[string]*flyproto.Field{},
	}

	set.cmdBase.init(kv, flyproto.CmdType_Set, c, seqno, req.Version, processDeadline, respDeadline, &s.wait4ReplyCount, set.makeResponse)

	for _, v := range req.GetFields() {
		set.fields[v.GetName()] = v
	}

	return set, nil
}
