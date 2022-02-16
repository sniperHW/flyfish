package flykv

import (
	"errors"
	//"fmt"
	"github.com/sniperHW/flyfish/db"
	"github.com/sniperHW/flyfish/errcode"
	flyproto "github.com/sniperHW/flyfish/proto"
	"sync/atomic"
)

func (this *dbUpdateTask) SetLastWriteBackVersion(version int64) {
	this.Lock()
	defer this.Unlock()
	this.state.LastWriteBackVersion = version
}

func (this *dbUpdateTask) GetTable() string {
	return this.kv.table
}

func (this *dbUpdateTask) isDoing() bool {
	this.Lock()
	defer this.Unlock()
	return this.doing
}

func (this *dbUpdateTask) CheckUpdateLease() bool {
	return this.kv.store.isLeader()
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
	return this.state.State != db.DBState_none
}

func (this *dbUpdateTask) ClearUpdateStateAndReleaseLock() {
	GetSugar().Infof("ClearUpdateStateAndReleaseLock")
	this.Lock()
	defer this.Unlock()
	this.doing = false
	this.state.Fields = map[string]*flyproto.Field{}
	this.state.State = db.DBState_none
	atomic.AddInt32(&this.kv.store.dbWriteBackCount, -1)
}

func (this *dbUpdateTask) GetUpdateAndClearUpdateState() (updateState db.UpdateState) {
	this.Lock()
	defer this.Unlock()
	updateState = this.state
	this.state.Fields = map[string]*flyproto.Field{}
	this.state.State = db.DBState_none
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

	this.state.Fields = map[string]*flyproto.Field{}

	switch this.kv.state {
	case kv_ok:
		this.state.State = db.DBState_insert
	case kv_no_record:
		this.state.State = db.DBState_delete
	case kv_new, kv_loading:
		return nil
	case kv_invaild:
		return errors.New("kv in invaild state")
	}

	this.state.Version = this.kv.version

	if this.state.State == db.DBState_insert {
		for k, v := range this.kv.fields {
			this.state.Fields[k] = v
		}
	}

	this.state.Meta = this.kv.meta
	this.doing = true
	atomic.AddInt32(&this.kv.store.dbWriteBackCount, 1)
	this.kv.store.db.issueUpdate(this) //这里不会出错，db要到最后才会stop

	return nil
}

func (this *dbUpdateTask) updateState(dbstate db.DBState, version int64, fields map[string]*flyproto.Field) error {

	if dbstate == db.DBState_none {
		return errors.New("updateState error 1")
	}

	this.Lock()
	defer this.Unlock()

	GetSugar().Debugf("updateState %s %d %d version:%d", this.kv.uniKey, dbstate, this.state.State, version)

	switch this.state.State {
	case db.DBState_none:
		this.state.State = dbstate
	case db.DBState_insert:
		if dbstate == db.DBState_update {
			this.state.State = db.DBState_insert
		} else if dbstate == db.DBState_delete {
			this.state.State = db.DBState_delete
			this.state.Fields = map[string]*flyproto.Field{}
		} else {
			return errors.New("updateState error 2")
		}
	case db.DBState_delete:
		if dbstate == db.DBState_insert {
			this.state.State = db.DBState_insert
		} else {
			return errors.New("updateState error 3")
		}
	case db.DBState_update:
		if dbstate == db.DBState_update || dbstate == db.DBState_delete {
			this.state.State = dbstate
			if dbstate == db.DBState_delete {
				this.state.Fields = map[string]*flyproto.Field{}
			}
		} else {
			return errors.New("updateState error 4")
		}
	default:
		return errors.New("updateState error 5")
	}

	this.state.Version = version
	this.state.Meta = this.kv.meta

	for k, v := range fields {
		this.state.Fields[k] = v
	}

	if !this.doing {
		this.doing = true
		atomic.AddInt32(&this.kv.store.dbWriteBackCount, 1)
		this.kv.store.db.issueUpdate(this)
	}

	return nil
}

func (this *dbLoadTask) GetTable() string {
	return this.table
}

func (this *dbLoadTask) GetKey() string {
	return this.key
}

func (this *dbLoadTask) GetUniKey() string {
	return this.uniKey
}

func (this *dbLoadTask) GetTableMeta() db.TableMeta {
	return this.meta
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

		if version <= 0 {
			err = db.ERR_RecordNotExist
		}
		/*
		 * 根据this.cmd产生正确的proposal
		 */
		proposal := &kvProposal{
			ptype:   proposal_snapshot,
			kv:      this.kv,
			cmds:    []cmdI{this.cmd},
			version: version,
			fields:  fields,
		}

		//GetSugar().Infof("load %s version:%d err:%v", this.kv.uniKey, version, err)

		this.kv.updateTask.SetLastWriteBackVersion(version)

		this.cmd.onLoadResult(err, proposal)

		this.kv.store.rn.IssueProposal(proposal)

	} else {
		this.onError(errcode.New(errcode.Errcode_error, err.Error()))
	}
}
