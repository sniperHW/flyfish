package kvnode

import (
	codec "github.com/sniperHW/flyfish/codec"
	"github.com/sniperHW/flyfish/errcode"
	"github.com/sniperHW/flyfish/proto"
)

type asynCmdTaskGet struct {
	*asynCmdTaskBase
}

func (this *asynCmdTaskGet) onSqlResp(errno int32) {
	this.asynCmdTaskBase.onSqlResp(errno)
	if errno == errcode.ERR_OK || errno == errcode.ERR_RECORD_NOTEXIST {
		//向副本同步插入操作
		this.getKV().store.issueAddkv(this)
	}
}

func newAsynCmdTaskGet() *asynCmdTaskGet {
	return &asynCmdTaskGet{
		asynCmdTaskBase: &asynCmdTaskBase{
			commands: []commandI{},
		},
	}
}

type cmdGet struct {
	*commandBase
	fields map[string]*proto.Field
}

func (this *cmdGet) reply(errCode int32, fields map[string]*proto.Field, version int64) {
	Debugln("cmdGet reply", errCode, version)
	this.replyer.reply(this, errCode, fields, version)
}

func (this *cmdGet) makeResponse(errCode int32, fields map[string]*proto.Field, version int64) *codec.Message {
	pbdata := &proto.GetResp{
		Version: version,
	}

	if errcode.ERR_OK == errCode {
		for _, field := range this.fields {
			v := fields[field.GetName()]
			if nil != v {
				pbdata.Fields = append(pbdata.Fields, v)
			} else {
				/*
				 * 表格新增加了列，但未设置过，使用默认值
				 */
				vv := this.kv.meta.GetDefaultV(field.GetName())
				if nil != vv {
					pbdata.Fields = append(pbdata.Fields, proto.PackField(field.GetName(), vv))
				}
			}
		}
	}

	return codec.NewMessage("", codec.CommonHead{
		Seqno:   this.replyer.seqno,
		ErrCode: errCode,
	}, pbdata)
}

func (this *cmdGet) prepare(t asynCmdTaskI) (asynCmdTaskI, bool) {

	task, ok := t.(*asynCmdTaskGet)

	if t != nil && !ok {
		//不同类命令不能合并
		return t, false
	}

	status := this.kv.getStatus()

	if nil == t {
		task = newAsynCmdTaskGet()
		if status == cache_missing || status == cache_ok {
			task.fields = this.kv.fields
			task.version = this.kv.version
			if status == cache_missing {
				task.errno = errcode.ERR_RECORD_NOTEXIST
			}
		}
	}

	task.commands = append(task.commands, this)

	return task, true
}

func get(n *KVNode, cli *cliConn, msg *codec.Message) {

	req := msg.GetData().(*proto.GetReq)

	head := msg.GetHead()

	processDeadline, respDeadline := getDeadline(head.Timeout)

	op := &cmdGet{
		commandBase: &commandBase{
			deadline: processDeadline,
			replyer:  newReplyer(cli, msg.GetHead().Seqno, respDeadline),
		},
		fields: map[string]*proto.Field{},
	}

	var err int32

	var kv *kv

	table, key := head.SplitUniKey()

	if kv, err = n.storeMgr.getkv(table, key, head.UniKey); errcode.ERR_OK != err {
		op.reply(err, nil, 0)
		return
	}

	op.kv = kv

	if req.GetAll() {
		for _, name := range op.kv.meta.GetQueryMeta().GetFieldNames() {
			if name != "__key__" && name != "__version__" {
				op.fields[name] = proto.PackField(name, nil)
			}
		}
	} else {
		for _, name := range req.GetFields() {
			op.fields[name] = proto.PackField(name, nil)
		}
	}

	if !kv.meta.CheckGet(op.fields) {
		op.reply(errcode.ERR_INVAILD_FIELD, nil, 0)
		return
	}

	if err = kv.appendCmd(op); err != errcode.ERR_OK {
		op.reply(err, nil, 0)
		return
	}

	kv.processQueueCmd()

}
