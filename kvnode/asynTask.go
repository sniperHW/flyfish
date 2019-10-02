package kvnode

import (
	"github.com/sniperHW/flyfish/dbmeta"
	"github.com/sniperHW/flyfish/errcode"
	"github.com/sniperHW/flyfish/proto"
	"sync/atomic"
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
	replyed  int64
}

type asynKickTask struct {
	kv *kv
}

func (this *asynCmdTaskBase) done() {
	kv.slot.RemoveKv(kv)
	kv.Lock()
	kv.setRemoveAndClearCmdQueue(errcode.ERR_RETRY)
	kv.Unlock()
	/*
		queueCmdSize := 0
		ckey := v.getCacheKey()
		ckey.mtx.Lock()
		queueCmdSize = ckey.cmdQueue.Len()
		if queueCmdSize > 0 {
			/*
			 *   kick执行完之后，对这个key又有新的访问请求
			 *   此时必须把snapshoted设置为true,这样后面的变更请求才能以snapshot记录到日志中
			 *   否则，重放日志时因为kick先执行，变更重放将因为找不到key出错
			 * /
			ckey.snapshoted = false
			ckey.kicking = false
		}
		ckey.mtx.Unlock()
		s.mtx.Lock()
		s.kickingCount--
		s.mtx.Unlock()
		if queueCmdSize > 0 {
			ckey.processQueueCmd()
		} else {
			s.mtx.Lock()
			s.removeLRU(ckey)
			s.keySize--
			s.mtx.Unlock()
			ckey.slot.mtx.Lock()
			delete(ckey.slot.kv, ckey.uniKey)
			ckey.slot.mtx.Unlock()
		}
	*/
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
			kv.processQueueCmd()
		}
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
