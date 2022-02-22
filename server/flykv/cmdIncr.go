package flykv

import (
	"github.com/sniperHW/flyfish/db"
	"github.com/sniperHW/flyfish/errcode"
	"github.com/sniperHW/flyfish/pkg/net"
	flyproto "github.com/sniperHW/flyfish/proto"
	"github.com/sniperHW/flyfish/proto/cs"
	"time"
)

type cmdIncr struct {
	cmdBase
	v *flyproto.Field
}

func (this *cmdIncr) makeResponse(err errcode.Error, fields map[string]*flyproto.Field, version int64) *cs.RespMessage {

	var field *flyproto.Field
	if err == nil {
		field = fields[this.v.GetName()]
	}

	return &cs.RespMessage{
		Seqno: this.seqno,
		Err:   err,
		Data: &flyproto.IncrByResp{
			Version: version,
			Field:   field,
		}}
}

func (this *cmdIncr) do(proposal *kvProposal) {
	if proposal.kvState == kv_no_record {
		proposal.version = abs(proposal.version) + 1
		//对于不在set中field,使用defalutValue填充
		proposal.fields = map[string]*flyproto.Field{}
		this.meta.FillDefaultValues(proposal.fields)
		proposal.dbstate = db.DBState_insert
		proposal.kvState = kv_ok
		proposal.ptype = proposal_snapshot

		oldV := this.kv.fields[this.v.GetName()]

		if nil == oldV {
			oldV = flyproto.PackField(this.v.GetName(), this.meta.GetDefaultValue(this.v.GetName()))
		}

		newV := flyproto.PackField(oldV.GetName(), oldV.GetInt()+this.v.GetInt())
		proposal.fields[this.v.GetName()] = newV

	} else {
		proposal.version++
		proposal.ptype = proposal_update
		proposal.dbstate = db.DBState_update
		oldV := this.kv.fields[this.v.GetName()]
		if nil == oldV {
			oldV = flyproto.PackField(this.v.GetName(), this.meta.GetDefaultValue(this.v.GetName()))
		}
		newV := flyproto.PackField(oldV.GetName(), oldV.GetInt()+this.v.GetInt())
		proposal.fields[this.v.GetName()] = newV
	}
	proposal.cmds = append(proposal.cmds, this)
}

func (s *kvstore) makeIncr(kv *kv, deadline time.Time, c *net.Socket, seqno int64, req *flyproto.IncrByReq) (cmdI, errcode.Error) {
	if nil == req.Field {
		return nil, errcode.New(errcode.Errcode_error, "field is nil")
	}

	if !req.Field.IsInt() {
		return nil, errcode.New(errcode.Errcode_error, "incrby accept int only")
	}

	if err := kv.meta.CheckFields(req.Field); nil != err {
		return nil, errcode.New(errcode.Errcode_error, err.Error())
	}

	incr := &cmdIncr{
		v: req.Field,
	}

	incr.cmdBase.init(kv, flyproto.CmdType_CompareAndSet, c, seqno, req.Version, deadline, &s.wait4ReplyCount, incr.makeResponse)

	return incr, nil
}
