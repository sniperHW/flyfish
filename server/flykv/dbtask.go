package flykv

import (
	"errors"
	"github.com/sniperHW/flyfish/db"
	"github.com/sniperHW/flyfish/errcode"
	flyproto "github.com/sniperHW/flyfish/proto"
	"sync/atomic"
)

func (this *dbUpdateTask) GetTable() string {
	return this.kv.table
}

func (this *dbUpdateTask) isDoing() bool {
	this.Lock()
	defer this.Unlock()
	return this.doing
}

func (this *dbUpdateTask) CheckUpdateLease() bool {
	return this.kv.store.hasLease()
}

func (this *dbUpdateTask) ReleaseLock() {
	this.Lock()
	defer this.Unlock()
	this.doing = false
	atomic.AddInt32(&this.kv.store.dbWriteBackCount, -1)
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
	atomic.AddInt32(&this.kv.store.dbWriteBackCount, -1)
}

func (this *dbUpdateTask) GetUpdateAndClearUpdateState() (updateState db.UpdateState) {
	this.Lock()
	defer this.Unlock()
	updateState.Version = this.version
	updateState.Fields = this.updateFields
	updateState.State = this.dbstate
	updateState.Meta = this.kv.meta
	updateState.Key = this.kv.key
	updateState.Slot = this.kv.slot
	this.updateFields = map[string]*flyproto.Field{}
	this.dbstate = db.DBState_none
	return
}

func (this *dbUpdateTask) GetUniKey() string {
	return this.kv.uniKey
}

func (this *dbUpdateTask) issueFullDbWriteBack() error {
	this.Lock()
	defer this.Unlock()

	if this.doing {
		return errors.New("is doing")
	}

	switch this.kv.state {
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

	this.version = this.kv.version

	if this.dbstate == db.DBState_insert {
		for k, v := range this.kv.fields {
			this.updateFields[k] = v
		}
	}

	this.doing = true
	atomic.AddInt32(&this.kv.store.dbWriteBackCount, 1)
	this.kv.store.db.issueUpdate(this) //这里不会出错，db要到最后才会stop

	return nil
}

func (this *dbUpdateTask) updateState(dbstate db.DBState, version int64, fields map[string]*flyproto.Field) error {

	if dbstate == db.DBState_none {
		return errors.New("updateState error 1")
	}

	if !this.kv.store.hasLease() {
		return nil
	}

	this.Lock()
	defer this.Unlock()

	GetSugar().Debugf("updateState %s %d %d", this.kv.uniKey, dbstate, this.dbstate)

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
		atomic.AddInt32(&this.kv.store.dbWriteBackCount, 1)
		this.kv.store.db.issueUpdate(this)
	}

	return nil
}

func (this *dbLoadTask) GetTable() string {
	return this.kv.table
}

func (this *dbLoadTask) GetKey() string {
	return this.kv.key
}

func (this *dbLoadTask) GetUniKey() string {
	return this.kv.uniKey
}

func (this *dbLoadTask) GetTableMeta() db.TableMeta {
	return this.kv.meta
}

func (this *dbLoadTask) onError(err errcode.Error) {
	this.cmd.reply(err, nil, 0)
	this.kv.store.mainQueue.AppendHighestPriotiryItem(func() {
		this.kv.store.deleteKv(this.kv)
		for f := this.kv.pendingCmd.front(); nil != f; f = this.kv.pendingCmd.front() {
			f.reply(err, nil, 0)
			this.kv.pendingCmd.popFront()
		}
	})
}

func (this *dbLoadTask) OnResult(err error, version int64, fields map[string]*flyproto.Field) {

	if !this.kv.store.isLeader() {
		this.onError(errcode.New(errcode.Errcode_not_leader))
	} else if err == nil || err == db.ERR_RecordNotExist {
		/*
		 * 根据this.cmd产生正确的proposal
		 */
		proposal := &kvProposal{
			ptype: proposal_snapshot,
			kv:    this.kv,
			cmds:  []cmdI{this.cmd},
		}

		if nil == err {
			proposal.fields = fields
			proposal.version = version
		}

		this.cmd.onLoadResult(err, proposal)

		this.kv.store.rn.IssueProposal(proposal)

	} else {
		this.onError(errcode.New(errcode.Errcode_error, err.Error()))
	}
}
