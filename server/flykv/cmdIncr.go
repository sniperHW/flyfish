package flykv

import (
	"github.com/sniperHW/flyfish/db"
	"github.com/sniperHW/flyfish/errcode"
	"github.com/sniperHW/flyfish/pkg/net/cs"
	flyproto "github.com/sniperHW/flyfish/proto"
	"time"
)

type cmdIncr struct {
	cmdBase
	tbmeta db.TableMeta
	v      *flyproto.Field
}

func (this *cmdIncr) makeResponse(err errcode.Error, fields map[string]*flyproto.Field, version int64) *cs.RespMessage {

	var field *flyproto.Field
	if err == nil {
		field = fields[this.v.GetName()]
	}

	return &cs.RespMessage{
		Seqno: this.seqno,
		Err:   err,
		Data: &flyproto.IncrByResp{
			Version: version,
			Field:   field,
		}}
}

func (this *cmdIncr) onLoadResult(err error, proposal *kvProposal) {
	if nil == err && nil != this.version && *this.version != proposal.version {
		this.reply(Err_version_mismatch, nil, 0)
	} else {
		if err == db.ERR_RecordNotExist {
			//记录不存在，为记录生成版本号
			proposal.version = genVersion()
			//对于不在set中field,使用defalutValue填充
			proposal.fields = map[string]*flyproto.Field{}
			this.tbmeta.FillDefaultValues(proposal.fields)
			proposal.dbstate = db.DBState_insert
		}

		oldV := proposal.fields[this.v.GetName()]

		if nil == oldV {
			oldV = flyproto.PackField(this.v.GetName(), this.tbmeta.GetDefaultValue(this.v.GetName()))
		}

		newV := flyproto.PackField(oldV.GetName(), oldV.GetInt()+this.v.GetInt())
		proposal.fields[this.v.GetName()] = newV
		if proposal.dbstate != db.DBState_insert {
			proposal.version = incVersion(proposal.version)
			proposal.dbstate = db.DBState_update
		}
	}
}

func (this *cmdIncr) do(keyvalue *kv, proposal *kvProposal) {
	if keyvalue.state == kv_no_record {
		//记录不存在，为记录生成版本号
		proposal.version = genVersion()
		//对于不在set中field,使用defalutValue填充
		proposal.fields = map[string]*flyproto.Field{}
		this.tbmeta.FillDefaultValues(proposal.fields)
		proposal.dbstate = db.DBState_insert

		oldV := proposal.fields[this.v.GetName()]

		if nil == oldV {
			oldV = flyproto.PackField(this.v.GetName(), this.tbmeta.GetDefaultValue(this.v.GetName()))
		}

		newV := flyproto.PackField(oldV.GetName(), oldV.GetInt()+this.v.GetInt())
		proposal.fields[this.v.GetName()] = newV

	} else {

		proposal.version = incVersion(proposal.version)
		proposal.dbstate = db.DBState_update

		oldV := keyvalue.fields[this.v.GetName()]

		if nil == oldV {
			oldV = flyproto.PackField(this.v.GetName(), this.tbmeta.GetDefaultValue(this.v.GetName()))
		}

		newV := flyproto.PackField(oldV.GetName(), oldV.GetInt()+this.v.GetInt())
		proposal.fields[this.v.GetName()] = newV
	}
}

func (s *kvstore) makeIncr(keyvalue *kv, processDeadline time.Time, respDeadline time.Time, c *conn, seqno int64, req *flyproto.IncrByReq) (cmdI, errcode.Error) {
	if nil == req.Field {
		return nil, errcode.New(errcode.Errcode_error, "field is nil")
	}

	if !req.Field.IsInt() {
		return nil, errcode.New(errcode.Errcode_error, "incrby accept int only")
	}

	if err := keyvalue.tbmeta.CheckFields(req.Field); nil != err {
		return nil, errcode.New(errcode.Errcode_error, err.Error())
	}

	incr := &cmdIncr{
		tbmeta: keyvalue.tbmeta,
		v:      req.Field,
	}

	initCmdBase(&incr.cmdBase, flyproto.CmdType_CompareAndSet, c, seqno, req.Version, processDeadline, respDeadline, &s.wait4ReplyCount, incr.makeResponse)

	return incr, nil
}
