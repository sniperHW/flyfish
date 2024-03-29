package flykv

import (
	"github.com/sniperHW/flyfish/errcode"
	flyproto "github.com/sniperHW/flyfish/proto"
	"github.com/sniperHW/flyfish/proto/cs"
	"time"
)

type cmdSet struct {
	cmdBase
	version *int64
	fields  map[string]*flyproto.Field
}

func (this *cmdSet) do(proposal *kvProposal) *kvProposal {
	if nil != this.version && *this.version != this.kv.version {
		this.reply(Err_version_mismatch, nil, 0)
		return nil
	} else {
		proposal.fields = map[string]*flyproto.Field{}
		if proposal.kvState == kv_no_record {
			proposal.version = abs(proposal.version) + 1
			proposal.kvState = kv_ok
			proposal.ptype = proposal_snapshot
			this.meta.FillDefaultValues(this.fields)
		} else {
			proposal.version++
			if proposal.ptype != proposal_snapshot {
				proposal.ptype = proposal_update
			}
		}

		for k, v := range this.fields {
			proposal.fields[k] = v
		}
		return proposal
	}
}

func (this *cmdSet) cmdType() flyproto.CmdType {
	return flyproto.CmdType_Set
}

func (this *cmdSet) checkVersion() bool {
	return this.version != nil
}

func (s *kvstore) makeSet(kv *kv, deadline time.Time, replyer *replyer, req *flyproto.SetReq) (cmdI, errcode.Error) {
	if len(req.GetFields()) == 0 {
		return nil, errcode.New(errcode.Errcode_error, "set fields is empty")
	}

	if err := kv.meta.CheckFields(req.GetFields()...); nil != err {
		return nil, errcode.New(errcode.Errcode_error, err.Error())
	}

	set := &cmdSet{
		version: req.Version,
		fields:  map[string]*flyproto.Field{},
	}

	for _, v := range req.GetFields() {
		set.fields[v.GetName()] = v
	}

	set.cmdBase.init(set, kv, replyer, deadline, func(err errcode.Error, _ map[string]*flyproto.Field, _ int64) *cs.RespMessage {
		return &cs.RespMessage{
			Err:  err,
			Data: &flyproto.SetResp{},
		}
	})

	return set, nil
}
