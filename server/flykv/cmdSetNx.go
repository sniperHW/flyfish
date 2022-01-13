package flykv

import (
	"github.com/sniperHW/flyfish/db"
	"github.com/sniperHW/flyfish/errcode"
	"github.com/sniperHW/flyfish/pkg/net"
	flyproto "github.com/sniperHW/flyfish/proto"
	"github.com/sniperHW/flyfish/proto/cs"
	"time"
)

type cmdSetNx struct {
	cmdBase
	fields map[string]*flyproto.Field
}

func (this *cmdSetNx) makeResponse(err errcode.Error, fields map[string]*flyproto.Field, version int64) *cs.RespMessage {
	pbdata := &flyproto.SetNxResp{
		Version: version,
	}

	if nil != err && err == Err_record_exist {
		for _, field := range this.fields {
			v := fields[field.GetName()]
			if nil != v {
				pbdata.Fields = append(pbdata.Fields, v)
			} else {
				/*
				 * 表格新增加了列，但未设置过，使用默认值
				 */
				vv := this.kv.meta.GetDefaultValue(field.GetName())
				if nil != vv {
					pbdata.Fields = append(pbdata.Fields, flyproto.PackField(field.GetName(), vv))
				}
			}
		}
	}

	return &cs.RespMessage{
		Seqno: this.seqno,
		Err:   err,
		Data:  pbdata}
}

func (this *cmdSetNx) onLoadResult(err error, proposal *kvProposal) {
	if err == db.ERR_RecordNotExist {
		//记录不存在，为记录生成版本号
		proposal.version = genVersion()
		//对于不在set中field,使用defalutValue填充
		this.kv.meta.FillDefaultValues(this.fields)
		proposal.fields = this.fields
		proposal.dbstate = db.DBState_insert
	} else if nil == err {
		this.reply(Err_record_exist, proposal.fields, proposal.version)
	}
}

func (this *cmdSetNx) do(proposal *kvProposal) {
	if this.kv.state == kv_no_record {
		proposal.version = genVersion()
		proposal.dbstate = db.DBState_insert
		this.kv.meta.FillDefaultValues(this.fields)
		proposal.fields = this.fields
	} else {
		proposal.ptype = proposal_none
		this.reply(Err_record_exist, this.kv.fields, this.kv.version)
	}
}

func (s *kvstore) makeSetNx(kv *kv, processDeadline time.Time, respDeadline time.Time, c *net.Socket, seqno int64, req *flyproto.SetNxReq) (cmdI, errcode.Error) {
	if len(req.GetFields()) == 0 {
		return nil, errcode.New(errcode.Errcode_error, "setNx fields is empty")
	}

	if err := kv.meta.CheckFields(req.GetFields()...); nil != err {
		return nil, errcode.New(errcode.Errcode_error, err.Error())
	}

	setNx := &cmdSetNx{
		fields: map[string]*flyproto.Field{},
	}

	setNx.cmdBase.init(kv, flyproto.CmdType_Set, c, seqno, req.Version, processDeadline, respDeadline, &s.wait4ReplyCount, setNx.makeResponse)

	for _, v := range req.GetFields() {
		setNx.fields[v.GetName()] = v
	}

	return setNx, nil
}
