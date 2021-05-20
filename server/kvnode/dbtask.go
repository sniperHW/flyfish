package kvnode

import (
	"errors"
	"github.com/sniperHW/flyfish/backend/db"
	"github.com/sniperHW/flyfish/errcode"
	flyproto "github.com/sniperHW/flyfish/proto"
)

func (this *dbUpdateTask) isDoing() bool {
	this.Lock()
	defer this.Unlock()
	return this.doing
}

func (this *dbUpdateTask) CheckUpdateLease() bool {
	return this.keyValue.store.lease.hasLease()
}

func (this *dbUpdateTask) ReleaseLock() {
	this.Lock()
	defer this.Unlock()
	this.doing = false
}

func (this *dbUpdateTask) Dirty() bool {
	this.Lock()
	defer this.Unlock()
	return this.dbstate != db.DBState_none
}

func (this *dbUpdateTask) ClearUpdateStateAndReleaseLock() {
	GetSugar().Infof("ClearUpdateStateAndReleaseLock")
	this.Lock()
	defer this.Unlock()
	this.updateFields = map[string]*flyproto.Field{}
	this.doing = false
	this.dbstate = db.DBState_none
}

func (this *dbUpdateTask) GetUpdateAndClearUpdateState() (updateState db.UpdateState) {
	this.Lock()
	defer this.Unlock()
	updateState.Version = this.version
	updateState.Fields = this.updateFields
	updateState.State = this.dbstate
	updateState.Meta = this.keyValue.tbmeta
	updateState.Key = this.keyValue.key
	this.updateFields = map[string]*flyproto.Field{}
	this.dbstate = db.DBState_none
	return
}

func (this *dbUpdateTask) GetUniKey() string {
	return this.keyValue.uniKey
}

func (this *dbUpdateTask) issueFullDbWriteBack() error {
	if !this.keyValue.store.hasLease() {
		return errors.New("not has lease")
	}

	this.Lock()
	defer this.Unlock()

	if this.doing {
		return errors.New("is doing")
	}

	switch this.keyValue.state {
	case kv_ok:
		this.dbstate = db.DBState_insert
	case kv_no_record:
		this.dbstate = db.DBState_delete
		this.updateFields = map[string]*flyproto.Field{}
	case kv_new, kv_loading:
		return nil
	case kv_invaild:
		return errors.New("kv in invaild state")
	}

	this.version = this.keyValue.version

	if this.dbstate == db.DBState_insert {
		for k, v := range this.keyValue.fields {
			this.updateFields[k] = v
		}
	}

	this.doing = true
	if !this.keyValue.store.db.issueUpdate(this) {
		this.doing = false
		return errors.New("issueUpdate failed")
	}

	return nil
}

func (this *dbUpdateTask) updateState(dbstate db.DBState, version int64, fields map[string]*flyproto.Field) error {

	if dbstate == db.DBState_none {
		return errors.New("updateState error 1")
	}

	if !this.keyValue.store.hasLease() {
		return nil
	}

	this.Lock()
	defer this.Unlock()

	GetSugar().Debugf("updateState %s %d %d", this.keyValue.uniKey, dbstate, this.dbstate)

	switch this.dbstate {
	case db.DBState_none:
		this.dbstate = dbstate
	case db.DBState_insert:
		if dbstate == db.DBState_update {
			this.dbstate = db.DBState_insert
		} else if dbstate == db.DBState_delete {
			this.dbstate = db.DBState_delete
			this.updateFields = map[string]*flyproto.Field{}
		} else {
			return errors.New("updateState error 2")
		}
	case db.DBState_delete:
		if dbstate == db.DBState_insert {
			this.dbstate = db.DBState_insert
		} else {
			return errors.New("updateState error 3")
		}
	case db.DBState_update:
		if dbstate == db.DBState_update || dbstate == db.DBState_delete {
			this.dbstate = dbstate
			if dbstate == db.DBState_delete {
				this.updateFields = map[string]*flyproto.Field{}
			}
		} else {
			return errors.New("updateState error 4")
		}
	default:
		return errors.New("updateState error 5")
	}

	this.version = version

	for k, v := range fields {
		this.updateFields[k] = v
	}

	if !this.doing {
		this.doing = true
		if !this.keyValue.store.db.issueUpdate(this) {
			this.doing = false
			return errors.New("issueUpdate failed")
		}
	}

	return nil
}

func (this *dbLoadTask) GetTable() string {
	return this.keyValue.tbmeta.TableName()
}

func (this *dbLoadTask) GetKey() string {
	return this.keyValue.key
}

func (this *dbLoadTask) GetUniKey() string {
	return this.keyValue.uniKey
}

func (this *dbLoadTask) GetTableMeta() db.TableMeta {
	return this.keyValue.tbmeta
}

func (this *dbLoadTask) onResultError(err errcode.Error) {
	this.cmd.reply(err, nil, 0)

	this.keyValue.store.mainQueue.AppendHighestPriotiryItem(func() {
		for f := this.keyValue.pendingCmd.front(); nil != f; f = this.keyValue.pendingCmd.front() {
			f.reply(err, nil, 0)
			this.keyValue.pendingCmd.popFront()
		}
		delete(this.keyValue.store.keyvals, this.keyValue.uniKey)
	})
}

func (this *dbLoadTask) OnResult(err error, version int64, fields map[string]*flyproto.Field) {

	if err == nil || err == db.ERR_RecordNotExist {
		/*
		 * 根据this.cmd产生正确的proposal
		 */
		proposal := &kvProposal{
			ptype:    proposal_snapshot,
			keyValue: this.keyValue,
			cmds:     []cmdI{this.cmd},
		}

		if nil == err {
			proposal.fields = fields
			proposal.version = version
		}

		this.cmd.onLoadResult(err, proposal)

		if err = this.keyValue.store.rn.IssueProposal(proposal); nil != err {
			GetSugar().Infof("reply retry")
			this.onResultError(errcode.New(errcode.Errcode_retry, "server is busy, please try again!"))
		}

	} else {
		this.onResultError(errcode.New(errcode.Errcode_error, err.Error()))
	}
}
