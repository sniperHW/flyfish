package flykv

import (
	//"github.com/sniperHW/flyfish/db"
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

	proposal.cmds = append(proposal.cmds, this)
}

func (this *cmdSet) cmdType() flyproto.CmdType {
	return flyproto.CmdType_Set
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

	set.cmdBase.init(kv, c, seqno, req.Version, deadline, &s.wait4ReplyCount, set.makeResponse)

	for _, v := range req.GetFields() {
		set.fields[v.GetName()] = v
	}

	return set, nil
}
