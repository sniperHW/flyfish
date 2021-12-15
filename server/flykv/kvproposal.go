package flykv

import (
	"github.com/sniperHW/flyfish/db"
	"github.com/sniperHW/flyfish/errcode"
	flyproto "github.com/sniperHW/flyfish/proto"
)

type kvProposal struct {
	proposalBase
	dbstate db.DBState
	ptype   proposalType
	fields  map[string]*flyproto.Field
	version int64
	cmds    []cmdI
	kv      *kv
}

type kvLinearizableRead struct {
	kv   *kv
	cmds []cmdI
}

func (this *kvProposal) Isurgent() bool {
	return false
}

func (this *kvProposal) OnError(err error) {

	GetSugar().Infof("kvProposal OnError:%v", err)

	for _, v := range this.cmds {
		v.reply(errcode.New(errcode.Errcode_error, err.Error()), nil, 0)
	}

	this.kv.store.mainQueue.AppendHighestPriotiryItem(func() {
		if this.kv.state == kv_loading {
			for f := this.kv.pendingCmd.front(); nil != f; f = this.kv.pendingCmd.front() {
				f.reply(errcode.New(errcode.Errcode_error, err.Error()), nil, 0)
				this.kv.pendingCmd.popFront()
			}
			this.kv.store.deleteKv(this.kv)
		} else {
			this.kv.process(nil)
		}
	})
}

func (this *kvProposal) Serilize(b []byte) []byte {
	return serilizeKv(b, this.ptype, this.kv.uniKey, this.version, this.fields)
}

func (this *kvProposal) apply() {
	if this.ptype == proposal_kick {
		for _, v := range this.cmds {
			v.reply(nil, nil, 0)
		}

		for f := this.kv.pendingCmd.front(); nil != f; f = this.kv.pendingCmd.front() {
			f.reply(errcode.New(errcode.Errcode_retry, "please try again"), nil, 0)
			this.kv.pendingCmd.popFront()
		}
		this.kv.store.deleteKv(this.kv)
	} else {

		oldState := this.kv.state

		if this.version == 0 {
			this.kv.state = kv_no_record
			this.kv.fields = nil
		} else {
			this.kv.state = kv_ok
		}

		this.kv.version = this.version
		if len(this.fields) > 0 {
			if nil == this.kv.fields {
				this.kv.fields = map[string]*flyproto.Field{}
			}
			for _, v := range this.fields {
				this.kv.fields[v.GetName()] = v
			}
		}

		for _, v := range this.cmds {
			v.reply(nil, this.kv.fields, this.version)
		}

		//update dbUpdateTask
		if this.dbstate != db.DBState_none {
			err := this.kv.updateTask.updateState(this.dbstate, this.version, this.fields)
			if nil != err {
				GetSugar().Errorf("%s updateState error:%v", this.kv.uniKey, err)
			}
		}

		if oldState == kv_loading {
			this.kv.store.lru.update(&this.kv.lru)
		}

		this.kv.process(nil)

	}
}

func (this *kvLinearizableRead) OnError(err error) {

	GetSugar().Errorf("kvLinearizableRead OnError:%v", err)

	for _, v := range this.cmds {
		GetSugar().Infof("reply retry")
		v.reply(errcode.New(errcode.Errcode_retry, "server is busy, please try again!"), nil, 0)
	}

	this.kv.store.mainQueue.AppendHighestPriotiryItem(func() {
		this.kv.process(nil)
	})
}

func (this *kvLinearizableRead) ok() {
	GetSugar().Debugf("kvLinearizableRead ok:%d version:%d", len(this.cmds), this.kv.version)

	for _, v := range this.cmds {
		v.reply(nil, this.kv.fields, this.kv.version)
	}

	this.kv.process(nil)
}
