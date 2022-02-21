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

func (this *cmdSet) do(proposal *kvProposal) {
	if this.kv.state == kv_no_record {
		proposal.version = abs(proposal.version) + 1
		proposal.dbstate = db.DBState_insert
		this.meta.FillDefaultValues(this.fields)
		proposal.fields = this.fields
	} else {
		proposal.ptype = proposal_update
		proposal.version++
		proposal.fields = this.fields
		proposal.dbstate = db.DBState_update
	}
}

func (s *kvstore) makeSet(kv *kv, deadline time.Time, c *net.Socket, seqno int64, req *flyproto.SetReq) (cmdI, errcode.Error) {
	if len(req.GetFields()) == 0 {
		return nil, errcode.New(errcode.Errcode_error, "set fields is empty")
	}

	if err := kv.meta.CheckFields(req.GetFields()...); nil != err {
		return nil, errcode.New(errcode.Errcode_error, err.Error())
	}

	set := &cmdSet{
		fields: map[string]*flyproto.Field{},
	}

	set.cmdBase.init(kv, flyproto.CmdType_Set, c, seqno, req.Version, deadline, &s.wait4ReplyCount, set.makeResponse)

	for _, v := range req.GetFields() {
		set.fields[v.GetName()] = v
	}

	return set, nil
}
