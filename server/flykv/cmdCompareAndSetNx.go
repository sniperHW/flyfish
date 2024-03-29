package flykv

import (
	"github.com/sniperHW/flyfish/errcode"
	flyproto "github.com/sniperHW/flyfish/proto"
	"github.com/sniperHW/flyfish/proto/cs"
	"time"
)

type cmdCompareAndSetNx struct {
	cmdBase
	old *flyproto.Field
	new *flyproto.Field
}

func (this *cmdCompareAndSetNx) do(proposal *kvProposal) *kvProposal {
	if proposal.kvState == kv_no_record {
		proposal.version = abs(proposal.version) + 1
		proposal.fields = map[string]*flyproto.Field{}
		this.meta.FillDefaultValues(proposal.fields)
		proposal.fields[this.new.GetName()] = this.new
		proposal.kvState = kv_ok
		proposal.ptype = proposal_snapshot
	} else {
		oldV := this.kv.getField(this.old.GetName())
		if !this.old.IsEqual(oldV) {
			this.reply(Err_cas_not_equal, this.kv.fields, 0)
			return nil
		} else {
			proposal.version++
			proposal.fields = map[string]*flyproto.Field{}
			proposal.fields[this.old.GetName()] = this.new
			proposal.ptype = proposal_update
		}
	}
	return proposal
}

func (this *cmdCompareAndSetNx) cmdType() flyproto.CmdType {
	return flyproto.CmdType_CompareAndSetNx
}

func (s *kvstore) makeCompareAndSetNx(kv *kv, deadline time.Time, replyer *replyer, req *flyproto.CompareAndSetNxReq) (cmdI, errcode.Error) {
	if req.New == nil {
		return nil, errcode.New(errcode.Errcode_error, "new is nil")
	}

	if req.Old == nil {
		return nil, errcode.New(errcode.Errcode_error, "old is nil")
	}

	if req.New.GetType() != req.Old.GetType() {
		return nil, errcode.New(errcode.Errcode_error, "new and old in different type")
	}

	if err := kv.meta.CheckFields(req.New); nil != err {
		return nil, errcode.New(errcode.Errcode_error, err.Error())
	}

	compareAndSetNx := &cmdCompareAndSetNx{
		new: req.New,
		old: req.Old,
	}

	compareAndSetNx.cmdBase.init(compareAndSetNx, kv, replyer, deadline, func(err errcode.Error, fields map[string]*flyproto.Field, _ int64) *cs.RespMessage {
		resp := &cs.RespMessage{
			Err:  err,
			Data: &flyproto.CompareAndSetNxResp{},
		}
		if err == Err_cas_not_equal {
			resp.Data.(*flyproto.CompareAndSetNxResp).Value = fields[req.Old.GetName()]
		}
		return resp
	})

	return compareAndSetNx, nil
}
