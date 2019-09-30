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

func fillDefaultValue(meta *dbmeta.TableMeta, fields *map[string]*proto.Field) {
	for name, v := range meta.GetFieldMetas() {
		if _, ok := (*fields)[name]; !ok {
			(*fields)[name] = proto.PackField(name, v.GetDefaultV())
		}
	}
}
