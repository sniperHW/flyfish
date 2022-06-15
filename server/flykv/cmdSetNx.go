package flykv

import (
	"github.com/sniperHW/flyfish/errcode"
	flyproto "github.com/sniperHW/flyfish/proto"
	"github.com/sniperHW/flyfish/proto/cs"
	"time"
)

type cmdSetNx struct {
	cmdBase
	fields map[string]*flyproto.Field
}

func (this *cmdSetNx) do(proposal *kvProposal) *kvProposal {
	if proposal.kvState == kv_no_record {
		proposal.version = abs(proposal.version) + 1
		proposal.kvState = kv_ok
		proposal.ptype = proposal_snapshot
		proposal.fields = map[string]*flyproto.Field{}
		this.meta.FillDefaultValues(this.fields)
		for k, v := range this.fields {
			proposal.fields[k] = v
		}
		return proposal
	} else {
		this.reply(Err_record_exist, this.kv.fields, this.kv.version)
		return nil
	}
}

func (this *cmdSetNx) cmdType() flyproto.CmdType {
	return flyproto.CmdType_SetNx
}

func (s *kvstore) makeSetNx(kv *kv, deadline time.Time, replyer *replyer, seqno int64, req *flyproto.SetNxReq) (cmdI, errcode.Error) {
	if len(req.GetFields()) == 0 {
		return nil, errcode.New(errcode.Errcode_error, "setNx fields is empty")
	}

	if err := kv.meta.CheckFields(req.GetFields()...); nil != err {
		return nil, errcode.New(errcode.Errcode_error, err.Error())
	}

	setNx := &cmdSetNx{
		fields: map[string]*flyproto.Field{},
	}

	for _, v := range req.GetFields() {
		setNx.fields[v.GetName()] = v
	}

	setNx.cmdBase.init(setNx, kv, replyer, seqno, deadline, func(err errcode.Error, fields map[string]*flyproto.Field, _ int64) *cs.RespMessage {
		pbdata := &flyproto.SetNxResp{}

		if nil != err && err == Err_record_exist {
			for name, field := range setNx.fields {
				if v := fields[name]; nil != v {
					pbdata.Fields = append(pbdata.Fields, v)
				} else {
					/*
					 * 表格新增加了列，但未设置过，使用默认值
					 */
					pbdata.Fields = append(pbdata.Fields, flyproto.PackField(name, setNx.meta.GetDefaultValue(field.GetName())))
				}
			}
		}

		return &cs.RespMessage{
			Seqno: seqno,
			Err:   err,
			Data:  pbdata}
	})

	return setNx, nil
}
