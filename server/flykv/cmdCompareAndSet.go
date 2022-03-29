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

func (this *cmdCompareAndSet) makeResponse(err errcode.Error, fields map[string]*flyproto.Field, version int64) *cs.RespMessage {
	var v *flyproto.Field

	if err == Err_cas_not_equal {
		v = fields[this.old.GetName()]
	}

	return &cs.RespMessage{
		Seqno: this.seqno,
		Err:   err,
		Data: &flyproto.CompareAndSetResp{
			Version: version,
			Value:   v,
		}}
}

func (this *cmdCompareAndSet) do(proposal *kvProposal) {
	if proposal.kvState == kv_no_record {
		this.reply(Err_record_notexist, nil, this.kv.version)
	} else {
		oldV := this.kv.getField(this.old.GetName())
		if !this.old.IsEqual(oldV) {
			this.reply(Err_cas_not_equal, this.kv.fields, this.kv.version)
		} else {
			proposal.version++
			proposal.fields = map[string]*flyproto.Field{}
			proposal.fields[this.old.GetName()] = this.new
			proposal.ptype = proposal_update
			proposal.cmds = append(proposal.cmds, this)
		}
	}
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

	compareAndSet.cmdBase.init(kv, replyer, seqno, req.Version, deadline, compareAndSet.makeResponse)

	return compareAndSet, nil
}
