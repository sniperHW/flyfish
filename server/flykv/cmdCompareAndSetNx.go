package flykv

import (
	//"github.com/sniperHW/flyfish/db"
	"github.com/sniperHW/flyfish/errcode"
	"github.com/sniperHW/flyfish/pkg/net"
	flyproto "github.com/sniperHW/flyfish/proto"
	"github.com/sniperHW/flyfish/proto/cs"
	"time"
)

type cmdCompareAndSetNx struct {
	cmdBase
	old *flyproto.Field
	new *flyproto.Field
}

func (this *cmdCompareAndSetNx) makeResponse(err errcode.Error, fields map[string]*flyproto.Field, version int64) *cs.RespMessage {
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

func (this *cmdCompareAndSetNx) do(proposal *kvProposal) {
	if proposal.kvState == kv_no_record {
		proposal.version = abs(proposal.version) + 1
		//对于不在set中field,使用defalutValue填充
		proposal.fields = map[string]*flyproto.Field{}
		proposal.fields[this.new.GetName()] = this.new
		this.meta.FillDefaultValues(proposal.fields)
		proposal.kvState = kv_ok
		proposal.ptype = proposal_snapshot
		proposal.cmds = append(proposal.cmds, this)
	} else {
		oldV := this.kv.fields[this.old.GetName()]
		if nil == oldV {
			oldV = flyproto.PackField(this.old.GetName(), this.meta.GetDefaultValue(this.old.GetName()))
		}
		if !this.old.IsEqual(oldV) {
			this.reply(Err_cas_not_equal, this.kv.fields, proposal.version)
		} else {
			proposal.version++
			proposal.fields = map[string]*flyproto.Field{}
			proposal.fields[this.old.GetName()] = this.new
			proposal.ptype = proposal_update
			proposal.cmds = append(proposal.cmds, this)
		}
	}
}

func (this *cmdCompareAndSetNx) cmdType() flyproto.CmdType {
	return flyproto.CmdType_CompareAndSetNx
}

func (s *kvstore) makeCompareAndSetNx(kv *kv, deadline time.Time, c *net.Socket, seqno int64, req *flyproto.CompareAndSetNxReq) (cmdI, errcode.Error) {
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

	compareAndSetNx.cmdBase.init(kv, c, seqno, req.Version, deadline, &s.wait4ReplyCount, compareAndSetNx.makeResponse)

	return compareAndSetNx, nil
}
