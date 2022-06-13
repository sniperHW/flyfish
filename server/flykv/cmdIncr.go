package flykv

import (
	"github.com/sniperHW/flyfish/errcode"
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

func (this *cmdIncr) do(proposal *kvProposal) *kvProposal {
	if proposal.kvState == kv_no_record {
		proposal.version = abs(proposal.version) + 1
		proposal.fields = map[string]*flyproto.Field{}
		this.meta.FillDefaultValues(proposal.fields)
		proposal.kvState = kv_ok
		proposal.ptype = proposal_snapshot
		oldV := proposal.fields[this.v.GetName()]
		newV := flyproto.PackField(oldV.GetName(), oldV.GetInt()+this.v.GetInt())
		proposal.fields[this.v.GetName()] = newV
	} else {
		proposal.version++
		proposal.ptype = proposal_update
		oldV := this.kv.getField(this.v.GetName())
		newV := flyproto.PackField(oldV.GetName(), oldV.GetInt()+this.v.GetInt())
		proposal.fields = map[string]*flyproto.Field{}
		proposal.fields[this.v.GetName()] = newV
	}
	return proposal
}

func (this *cmdIncr) cmdType() flyproto.CmdType {
	return flyproto.CmdType_IncrBy
}

func (s *kvstore) makeIncr(kv *kv, deadline time.Time, replyer *replyer, seqno int64, req *flyproto.IncrByReq) (cmdI, errcode.Error) {
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

	incr.cmdBase.init(incr, kv, replyer, seqno, req.Version, deadline, incr.makeResponse)

	return incr, nil
}
