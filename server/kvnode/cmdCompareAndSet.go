package kvnode

import (
	"github.com/sniperHW/flyfish/backend/db"
	"github.com/sniperHW/flyfish/errcode"
	"github.com/sniperHW/flyfish/net/cs"
	flyproto "github.com/sniperHW/flyfish/proto"
	"time"
)

type cmdCompareAndSet struct {
	cmdBase
	tbmeta db.TableMeta
	old    *flyproto.Field
	new    *flyproto.Field
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

func (this *cmdCompareAndSet) onLoadResult(err error, proposal *kvProposal) {
	if nil == err && nil != this.version && *this.version != proposal.version {
		this.reply(Err_version_mismatch, nil, 0)
	} else {
		if err == db.ERR_RecordNotExist {
			this.reply(Err_record_notexist, nil, 0)
		} else {
			oldV := proposal.fields[this.old.GetName()]
			hasChange := false
			if nil == oldV {
				oldV = flyproto.PackField(this.old.GetName(), this.tbmeta.GetDefaultValue(this.old.GetName()))
				proposal.fields[this.old.GetName()] = oldV
				hasChange = true
			}

			if !this.old.IsEqual(oldV) {
				this.reply(Err_cas_not_equal, proposal.fields, proposal.version)
				if hasChange {
					proposal.dbstate = db.DBState_update
				}
			} else {
				proposal.version = incVersion(proposal.version)
				proposal.fields[this.old.GetName()] = this.new
				proposal.dbstate = db.DBState_update
			}
		}
	}
}

func (this *cmdCompareAndSet) do(keyvalue *kv, proposal *kvProposal) {
	if keyvalue.state == kv_no_record {
		this.reply(Err_record_notexist, nil, 0)
		proposal.ptype = proposal_none
	} else {
		oldV := keyvalue.fields[this.old.GetName()]
		hasChange := false
		if nil == oldV {
			oldV = flyproto.PackField(this.old.GetName(), this.tbmeta.GetDefaultValue(this.old.GetName()))
			hasChange = true
		}
		if !this.old.IsEqual(oldV) {
			proposal.fields[this.old.GetName()] = oldV
			this.reply(Err_cas_not_equal, proposal.fields, proposal.version)
			if hasChange {
				proposal.dbstate = db.DBState_update
			} else {
				proposal.ptype = proposal_none
			}
		} else {
			proposal.version = incVersion(proposal.version)
			proposal.fields[this.old.GetName()] = this.new
			proposal.dbstate = db.DBState_update
		}
	}
}

func (s *kvstore) makeCompareAndSet(keyvalue *kv, processDeadline time.Time, respDeadline time.Time, c *conn, seqno int64, req *flyproto.CompareAndSetReq) (cmdI, errcode.Error) {
	if req.New == nil {
		return nil, errcode.New(errcode.Errcode_error, "new is nil")
	}

	if req.Old == nil {
		return nil, errcode.New(errcode.Errcode_error, "old is nil")
	}

	if req.New.GetType() != req.Old.GetType() {
		return nil, errcode.New(errcode.Errcode_error, "new and old in different type")
	}

	if err := keyvalue.tbmeta.CheckFields(req.New); nil != err {
		return nil, errcode.New(errcode.Errcode_error, err.Error())
	}

	compareAndSet := &cmdCompareAndSet{
		tbmeta: keyvalue.tbmeta,
		new:    req.New,
		old:    req.Old,
	}

	initCmdBase(&compareAndSet.cmdBase, flyproto.CmdType_CompareAndSet, c, seqno, req.Version, processDeadline, respDeadline, &s.wait4ReplyCount, compareAndSet.makeResponse)

	return compareAndSet, nil
}
