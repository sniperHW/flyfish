package flykv

import (
	"github.com/sniperHW/flyfish/db"
	"github.com/sniperHW/flyfish/errcode"
	"github.com/sniperHW/flyfish/pkg/net"
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
	if this.kv.state == kv_no_record {
		this.reply(Err_record_notexist, nil, 0)
		proposal.ptype = proposal_none
	} else {
		oldV := this.kv.fields[this.old.GetName()]
		if nil == oldV {
			oldV = flyproto.PackField(this.old.GetName(), this.meta.GetDefaultValue(this.old.GetName()))
		}
		if !this.old.IsEqual(oldV) {
			this.reply(Err_cas_not_equal, proposal.fields, proposal.version)
			proposal.ptype = proposal_none
		} else {
			proposal.version++
			proposal.fields[this.old.GetName()] = this.new
			proposal.dbstate = db.DBState_update
		}
	}
}

func (s *kvstore) makeCompareAndSet(kv *kv, deadline time.Time, c *net.Socket, seqno int64, req *flyproto.CompareAndSetReq) (cmdI, errcode.Error) {
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

	compareAndSet.cmdBase.init(kv, flyproto.CmdType_CompareAndSet, c, seqno, req.Version, deadline, &s.wait4ReplyCount, compareAndSet.makeResponse)

	return compareAndSet, nil
}
