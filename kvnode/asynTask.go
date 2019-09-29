package kvnode

import (
	"github.com/sniperHW/flyfish/dbmeta"
	"github.com/sniperHW/flyfish/errcode"
	"github.com/sniperHW/flyfish/proto"
)

type asynTaskI interface {
	done()
}

type asynCmdTaskI interface {
	getKV() *kv
	reply()
	dontReply()
	onLoadField(*proto.Field)
	onSqlResp(errno int32)
	getCommands() []commandI
}

type asynCmdTaskBase struct {
	commands []commandI
	fields   map[string]*proto.Field
	sqlFlag  uint32
	version  int64
	errno    int32
}

func (this *asynCmdTaskBase) getCommands() []commandI {
	return this.commands
}

func (this *asynCmdTaskBase) getKV() *kv {
	return this.commands[0].getKV()
}

func (this *asynCmdTaskBase) reply() {
	for _, v := range this.commands {
		v.reply(this.errno, this.fields, this.version)
	}
}

func (this *asynCmdTaskBase) dontReply() {
	for _, v := range this.commands {
		v.dontReply()
	}
}

func (this *asynCmdTaskBase) onLoadField(v *proto.Field) {
	if nil == this.fields {
		this.fields = map[string]*proto.Field{}
	}
	this.fields[v.GetName()] = v
}

func (this *asynCmdTaskBase) onSqlResp(errno int32) {
	this.errno = errno
	if errno == errcode.ERR_OK {
		this.version = this.fields["__version__"].GetInt()
	} else if errno == errcode.ERR_RECORD_NOTFOUND {
		this.reply()
	} else {
		this.reply()
		kv := this.getKV()
		if !kv.tryRemoveTmpKey(errcode.ERR_SQLERROR) {
			kv.processQueueCmd()
		}
	}
}

func (this *asynCmdTaskBase) done() {

}

type asynCmdTaskGet struct {
	*asynCmdTaskBase
}

func (this *asynCmdTaskGet) onSqlResp(errno int32) {
	this.asynCmdTaskBase.onSqlResp(errno)
	if errno == errcode.ERR_OK || errno == errcode.ERR_RECORD_NOTFOUND {
		//向副本同步插入操作
		//ckey.store.issueAddKv(ctx)
	}
}

func newAsynCmdTaskGet() *asynCmdTaskGet {
	return &asynCmdTaskGet{
		asynCmdTaskBase: &asynCmdTaskBase{
			commands: []commandI{},
		},
	}
}

type asynCmdTaskCompareAndSet struct {
	*asynCmdTaskBase
}

func (this *asynCmdTaskCompareAndSet) onSqlResp(errno int32) {
	this.asynCmdTaskBase.onSqlResp(errno)
	if errno == errcode.ERR_RECORD_NOTFOUND {
		//向副本同步插入操作
		//ckey.store.issueAddKv(ctx)
	} else if errno == errcode.ERR_OK {

	}
}

func newAsynCmdTaskCompareAndSet(cmd commandI) *asynCmdTaskCompareAndSet {
	return &asynCmdTaskCompareAndSet{
		asynCmdTaskBase: &asynCmdTaskBase{
			commands: []commandI{cmd},
		},
	}
}

type asynCmdTaskDel struct {
	*asynCmdTaskBase
}

func (this *asynCmdTaskDel) onSqlResp(errno int32) {
	this.asynCmdTaskBase.onSqlResp(errno)
	if errno == errcode.ERR_RECORD_NOTFOUND {
		//向副本同步插入操作
		//ckey.store.issueAddKv(ctx)
	} else if errno == errcode.ERR_OK {

	}
}

func newAsynCmdTaskDel(cmd commandI) *asynCmdTaskDel {
	return &asynCmdTaskDel{
		asynCmdTaskBase: &asynCmdTaskBase{
			commands: []commandI{cmdI},
		},
	}
}
