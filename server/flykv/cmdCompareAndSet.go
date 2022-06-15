package flykv

import (
	"github.com/sniperHW/flyfish/errcode"
	flyproto "github.com/sniperHW/flyfish/proto"
	"github.com/sniperHW/flyfish/proto/cs"
	"time"
)

type cmdCompareAndSet struct {
	cmdBase
	old *flyproto.Field
	new *flyproto.Field
}

func (this *cmdCompareAndSet) do(proposal *kvProposal) *kvProposal {
	if proposal.kvState == kv_no_record {
		this.reply(Err_record_notexist, nil, 0)
		return nil
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

func (this *cmdCompareAndSet) cmdType() flyproto.CmdType {
	return flyproto.CmdType_CompareAndSet
}

func (s *kvstore) makeCompareAndSet(kv *kv, deadline time.Time, replyer *replyer, seqno int64, req *flyproto.CompareAndSetReq) (cmdI, errcode.Error) {
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

	compareAndSet := &cmdCompareAndSet{
		new: req.New,
		old: req.Old,
	}

	compareAndSet.cmdBase.init(compareAndSet, kv, replyer, seqno, deadline, func(err errcode.Error, fields map[string]*flyproto.Field, _ int64) *cs.RespMessage {
		resp := &cs.RespMessage{
			Seqno: seqno,
			Err:   err,
			Data:  &flyproto.CompareAndSetResp{},
		}
		if err == Err_cas_not_equal {
			resp.Data.(*flyproto.CompareAndSetResp).Value = fields[req.Old.GetName()]
		}
		return resp
	})

	return compareAndSet, nil
}
