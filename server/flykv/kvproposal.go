package flykv

import (
	"github.com/sniperHW/flyfish/backend/db"
	"github.com/sniperHW/flyfish/errcode"
	"github.com/sniperHW/flyfish/pkg/buffer"
	flyproto "github.com/sniperHW/flyfish/proto"
)

type kvProposal struct {
	dbstate  db.DBState
	ptype    proposalType
	fields   map[string]*flyproto.Field
	version  int64
	cmds     []cmdI
	keyValue *kv
}

type kvLinearizableRead struct {
	keyValue *kv
	cmds     []cmdI
}

func (this *kvProposal) Isurgent() bool {
	return false
}

func (this *kvProposal) OnError(err error) {

	GetSugar().Infof("kvProposal OnError:%v", err)

	for _, v := range this.cmds {
		v.reply(errcode.New(errcode.Errcode_error, err.Error()), nil, 0)
	}

	this.keyValue.store.mainQueue.AppendHighestPriotiryItem(func() {
		if this.keyValue.state == kv_loading {
			for f := this.keyValue.pendingCmd.front(); nil != f; f = this.keyValue.pendingCmd.front() {
				f.reply(errcode.New(errcode.Errcode_error, err.Error()), nil, 0)
				this.keyValue.pendingCmd.popFront()
			}
			delete(this.keyValue.store.keyvals[this.keyValue.groupID].kv, this.keyValue.uniKey)
		} else {
			this.keyValue.process(nil)
		}
	})
}

func (this *kvProposal) Serilize(b []byte) []byte {
	return serilizeKv(b, this.ptype, this.keyValue.uniKey, this.version, this.fields)
}

func (this *kvProposal) OnMergeFinish(b []byte) (ret []byte) {
	if len(b) >= 1024 {
		c := getCompressor()
		cb, err := c.Compress(b)
		if nil != err {
			ret = buffer.AppendByte(b, byte(0))
		} else {
			b = b[:0]
			b = buffer.AppendBytes(b, cb)
			ret = buffer.AppendByte(b, byte(1))
		}
		releaseCompressor(c)
	} else {
		ret = buffer.AppendByte(b, byte(0))
	}
	return
}

func (this *kvProposal) apply() {
	if this.ptype == proposal_kick {
		for _, v := range this.cmds {
			v.reply(nil, nil, 0)
		}

		for f := this.keyValue.pendingCmd.front(); nil != f; f = this.keyValue.pendingCmd.front() {
			GetSugar().Infof("reply retry")
			f.reply(errcode.New(errcode.Errcode_retry, "please try again"), nil, 0)
			this.keyValue.pendingCmd.popFront()
		}

		delete(this.keyValue.store.keyvals[this.keyValue.groupID].kv, this.keyValue.uniKey)

		this.keyValue.store.keyvals[this.keyValue.groupID].kicks[this.keyValue.uniKey] = true

		//从LRU删除
		this.keyValue.store.lru.removeLRU(&this.keyValue.lru)

	} else {

		oldState := this.keyValue.state

		if this.version == 0 {
			this.keyValue.state = kv_no_record
			this.keyValue.fields = nil
		} else {
			this.keyValue.state = kv_ok
		}

		this.keyValue.version = this.version
		if len(this.fields) > 0 {
			if nil == this.keyValue.fields {
				this.keyValue.fields = map[string]*flyproto.Field{}
			}
			for _, v := range this.fields {
				this.keyValue.fields[v.GetName()] = v
			}
		}

		for _, v := range this.cmds {
			v.reply(nil, this.keyValue.fields, this.version)
		}

		//update dbUpdateTask
		if this.dbstate != db.DBState_none {
			err := this.keyValue.updateTask.updateState(this.dbstate, this.version, this.fields)
			if nil != err {
				GetSugar().Errorf("%s updateState error:%v", this.keyValue.uniKey, err)
			}
		}

		if oldState == kv_loading {
			this.keyValue.store.lru.updateLRU(&this.keyValue.lru)
			delete(this.keyValue.store.keyvals[this.keyValue.groupID].kicks, this.keyValue.uniKey)
		}

		this.keyValue.snapshot = true
		this.keyValue.process(nil)

	}
}

func (this *kvLinearizableRead) OnError(err error) {

	GetSugar().Errorf("kvLinearizableRead OnError:%v", err)

	for _, v := range this.cmds {
		GetSugar().Infof("reply retry")
		v.reply(errcode.New(errcode.Errcode_retry, "server is busy, please try again!"), nil, 0)
	}

	this.keyValue.store.mainQueue.AppendHighestPriotiryItem(func() {
		this.keyValue.process(nil)
	})
}

func (this *kvLinearizableRead) ok() {
	GetSugar().Debugf("kvLinearizableRead ok:%d version:%d", len(this.cmds), this.keyValue.version)

	for _, v := range this.cmds {
		v.reply(nil, this.keyValue.fields, this.keyValue.version)
	}

	this.keyValue.process(nil)
}
