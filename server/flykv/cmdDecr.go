package flykv

import (
	"github.com/sniperHW/flyfish/errcode"
	flyproto "github.com/sniperHW/flyfish/proto"
	"github.com/sniperHW/flyfish/proto/cs"
	"time"
)

type cmdDecr struct {
	cmdBase
	v *flyproto.Field
}

func (this *cmdDecr) makeResponse(err errcode.Error, fields map[string]*flyproto.Field, version int64) *cs.RespMessage {

	var field *flyproto.Field
	if err == nil {
		field = fields[this.v.GetName()]
	}

	return &cs.RespMessage{
		Seqno: this.seqno,
		Err:   err,
		Data: &flyproto.DecrByResp{
			Version: version,
			Field:   field,
		}}
}

func (this *cmdDecr) do(proposal *kvProposal) {
	if proposal.kvState == kv_no_record {
		proposal.version = abs(proposal.version) + 1
		proposal.fields = map[string]*flyproto.Field{}
		this.meta.FillDefaultValues(proposal.fields)
		proposal.kvState = kv_ok
		proposal.ptype = proposal_snapshot
		oldV := proposal.fields[this.v.GetName()]
		newV := flyproto.PackField(oldV.GetName(), oldV.GetInt()-this.v.GetInt())
		proposal.fields[this.v.GetName()] = newV

	} else {
		proposal.version++
		proposal.ptype = proposal_update
		oldV := this.kv.getField(this.v.GetName())
		newV := flyproto.PackField(oldV.GetName(), oldV.GetInt()-this.v.GetInt())
		proposal.fields = map[string]*flyproto.Field{}
		proposal.fields[this.v.GetName()] = newV
	}
	proposal.cmds = append(proposal.cmds, this)
}

func (this *cmdDecr) cmdType() flyproto.CmdType {
	return flyproto.CmdType_DecrBy
}

func (s *kvstore) makeDecr(kv *kv, deadline time.Time, replyer *replyer, seqno int64, req *flyproto.DecrByReq) (cmdI, errcode.Error) {
	if nil == req.Field {
		return nil, errcode.New(errcode.Errcode_error, "field is nil")
	}

	if !req.Field.IsInt() {
		return nil, errcode.New(errcode.Errcode_error, "incrby accept int only")
	}

	if err := kv.meta.CheckFields(req.Field); nil != err {
		return nil, errcode.New(errcode.Errcode_error, err.Error())
	}

	decr := &cmdDecr{
		v: req.Field,
	}

	decr.cmdBase.init(kv, replyer, seqno, req.Version, deadline, decr.makeResponse)

	return decr, nil
}
