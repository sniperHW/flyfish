package flykv

import (
	"errors"
	"github.com/sniperHW/flyfish/db"
	"github.com/sniperHW/flyfish/errcode"
	flyproto "github.com/sniperHW/flyfish/proto"
	"sync/atomic"
)

func (this *dbUpdateTask) SetLastWriteBackVersion(version int64) {
	//先设置让后面的更新能尽快看到最新值
	this.setLastWriteBackVersion(version)
	this.kv.store.rn.IssueProposal(&LastWriteBackVersionProposal{
		version: version,
		kv:      this.kv,
	})
}

func (this *dbUpdateTask) setLastWriteBackVersion(version int64) {
	this.Lock()
	defer this.Unlock()
	/*
	 * 考虑如下情况,leader db回写成功，并执行SetLastWriteBackVersion。
	 * 之后丢失leader，所以IssueProposal(LastWriteBackVersionProposal)失败，因此，
	 * kv.lastWriteBackVersion不会被更新成最新值。之后当前节点再次被选为leader,
	 * 在issueFullDbWriteBack中将用kv.lastWriteBackVersion再次调用setLastWriteBackVersion
	 * 此时kv.lastWriteBackVersion是比dbUpdateTask.state.LastWriteBackVersion旧的，应该忽略此次设置
	 */
	if abs(version) > abs(this.state.LastWriteBackVersion) {
		this.state.LastWriteBackVersion = version
	}
	return
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
	this.Lock()
	defer this.Unlock()
	this.doing = false
	this.state.State = db.DBState_none
	this.state.Fields = nil
	atomic.AddInt32(&this.kv.store.dbWriteBackCount, -1)
}

func (this *dbUpdateTask) GetUpdateAndClearUpdateState() (updateState db.UpdateState) {
	this.Lock()
	defer this.Unlock()
	updateState = this.state
	this.state.Fields = nil
	this.state.State = db.DBState_none
	return
}

func (this *dbUpdateTask) GetUniKey() string {
	return this.kv.uniKey
}

func (this *dbUpdateTask) issueUpdate() {
	if !this.doing {
		GetSugar().Debugf("%s issueUpdate", this.kv.uniKey)
		this.doing = true
		atomic.AddInt32(&this.kv.store.dbWriteBackCount, 1)
		this.kv.store.db.issueUpdate(this) //这里不会出错，db要到最后才会stop
	}
}

func (this *dbUpdateTask) _issueFullDbWriteBack() error {
	if this.kv.version == this.kv.lastWriteBackVersion || this.doing {
		return nil
	}

	switch this.kv.state {
	case kv_ok, kv_no_record:
	case kv_new, kv_loading:
		return nil
	case kv_invaild:
		return errors.New("kv in invaild state")
	}

	if this.kv.lastWriteBackVersion == 0 {
		this.state.State = db.DBState_insert
	} else if this.kv.state == kv_ok {
		this.state.State = db.DBState_update
	} else {
		this.state.State = db.DBState_delete
	}

	this.state.Version = this.kv.version
	this.state.LastWriteBackVersion = this.kv.version

	if this.state.State != db.DBState_delete {
		this.state.Fields = map[string]*flyproto.Field{}
		if nil != this.kv.fields {
			for k, v := range this.kv.fields {
				this.state.Fields[k] = v
			}
		}
	}

	this.state.Meta = this.kv.meta
	this.issueUpdate()
	return nil
}

func (this *dbUpdateTask) issueFullDbWriteBack() error {
	this.Lock()
	defer this.Unlock()
	return this._issueFullDbWriteBack()
}

func (this *dbUpdateTask) issueKickDbWriteBack() {
	this.Lock()
	defer this.Unlock()
	if this.kv.version != this.kv.lastWriteBackVersion {
		if this.state.State == db.DBState_none {
			this._issueFullDbWriteBack()
		} else {
			this.issueUpdate()
		}
	}
}

func (this *dbUpdateTask) updateState(dbstate db.DBState, version int64, fields map[string]*flyproto.Field) error {

	this.Lock()
	defer this.Unlock()

	GetSugar().Debugf("updateState %s %d %d version:%d", this.kv.uniKey, dbstate, this.state.State, version)

	this.state.State = dbstate
	this.state.Version = version
	this.state.Meta = this.kv.meta

	if nil != fields {
		if nil == this.state.Fields {
			this.state.Fields = map[string]*flyproto.Field{}
		}
		for k, v := range fields {
			this.state.Fields[k] = v
		}
	}

	if this.kv.store.kvnode.writeBackMode == write_through {
		this.issueUpdate()
	}

	return nil
}

func (this *dbUpdateTask) OnError(err error) {
	GetSugar().Errorf("dbUpdateTask OnError uniKey:%s err:%v", this.kv.uniKey, err)
	this.kv.store.mainQueue.AppendHighestPriotiryItem(func() {
		if f := this.kv.pendingCmd.Front(); nil != f {
			if cmdkick, ok := f.Value.(*cmdKick); ok {
				//如果有等待回写后执行的kick，需要清理一下
				cmdkick.reply(errcode.New(errcode.Errcode_error, err.Error()), nil, this.kv.version)
				this.kv.pendingCmd.Remove(f)
				this.kv.processCmd()
			}
		}
	})
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
	this.kv.store.mainQueue.AppendHighestPriotiryItem(func() {
		this.kv.clearCmds(err)
		this.kv.store.deleteKv(this.kv)
	})
}

func (this *dbLoadTask) OnResult(err error, version int64, fields map[string]*flyproto.Field) {
	if !this.kv.store.isLeader() {
		this.onError(errcode.New(errcode.Errcode_not_leader))
	} else if this.kv.store.getTerm() != this.term {
		/* 考虑如下情况
		 * store发起load,之后降级为follower,执行清理，然后再次成为leader，之后load返回
		 * 此时的leader已经不是之前发起load时的leader,因此不应继续执行下去
		 */
		return
	} else if err == nil || err == db.ERR_RecordNotExist {
		proposal := &kvProposal{
			ptype:       proposal_snapshot,
			kv:          this.kv,
			version:     version,
			fields:      fields,
			causeByLoad: true,
			dbversion:   version,
		}

		GetSugar().Debugf("dbLoadTask.OnResult %s %d", this.kv.uniKey, version)

		if version <= 0 {
			proposal.kvState = kv_no_record
		} else {
			proposal.kvState = kv_ok
		}

		this.kv.store.rn.IssueProposal(proposal)

	} else {
		this.onError(errcode.New(errcode.Errcode_error, err.Error()))
	}
}
