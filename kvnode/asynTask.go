package kvnode

import (
	"github.com/sniperHW/flyfish/dbmeta"
	"github.com/sniperHW/flyfish/errcode"
	"github.com/sniperHW/flyfish/proto"
	"sync/atomic"
)

type asynTaskI interface {
	done()
	onError(errno int32)
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
	replyed  int64
}

type asynKickTask struct {
	kv *kv
}

func (this *asynCmdTaskBase) getCommands() []commandI {
	return this.commands
}

func (this *asynCmdTaskBase) getKV() *kv {
	return this.commands[0].getKV()
}

func (this *asynCmdTaskBase) reply() {
	if atomic.CompareAndSwapInt64(&this.replyed, 0, 1) {
		for _, v := range this.commands {
			v.reply(this.errno, this.fields, this.version)
		}
	}
}

func (this *asynCmdTaskBase) dontReply() {
	if atomic.CompareAndSwapInt64(&this.replyed, 0, 1) {
		for _, v := range this.commands {
			v.dontReply()
		}
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
	} else if errno == errcode.ERR_SQLERROR {
		this.reply()
		kv := this.getKV()
		if !kv.tryRemoveTmpKey(this.errno) {
			kv.processQueueCmd(true)
		}
	}
}

func (this *asynCmdTaskBase) onError(errno int32) {
	this.errno = errno
	this.reply()
	kv := this.getKV()
	if !kv.tryRemoveTmpKey(this.errno) {
		kv.processQueueCmd(true)
	}
}

func (this *asynCmdTaskBase) done() {
	kv := this.getKV()
	kv.Lock()
	kv.setSnapshoted(true)
	isTmp := kv.isTmp()
	sqlFlag := kv.getSqlFlag()
	switch sqlFlag {
	case sql_insert, sql_update, sql_insert_update:
		kv.setOK(this.version, this.fields)
	case sql_delete:
		kv.setMissing()
	}
	if sqlFlag != sql_none && !kv.isWriteBack() {
		kv.setWriteBack(true)
		kv.slot.getKvNode().sqlMgr.pushUpdateReq(kv)
	}
	kv.Unlock()
	if isTmp {
		kv.slot.moveTmp2OK(kv)
	}
	kv.processQueueCmd(true)
}

func fillDefaultValue(meta *dbmeta.TableMeta, fields *map[string]*proto.Field) {
	for name, v := range meta.GetFieldMetas() {
		if _, ok := (*fields)[name]; !ok {
			(*fields)[name] = proto.PackField(name, v.GetDefaultV())
		}
	}
}
