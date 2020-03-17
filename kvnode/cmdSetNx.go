package kvnode

import (
	codec "github.com/sniperHW/flyfish/codec"
	"github.com/sniperHW/flyfish/errcode"
	"github.com/sniperHW/flyfish/proto"
)

type asynCmdTaskSetNx struct {
	*asynCmdTaskBase
}

func (this *asynCmdTaskSetNx) onSqlResp(errno int32) {
	this.asynCmdTaskBase.onSqlResp(errno)
	cmd := this.commands[0].(*cmdSetNx)

	if errno == errcode.ERR_RECORD_NOTEXIST {
		this.fields = map[string]*proto.Field{}
		fillDefaultValue(cmd.getKV().meta, &this.fields)
		for k, v := range cmd.fields {
			this.fields[k] = v
		}
		this.errno = errcode.ERR_OK
		this.sqlFlag = sql_insert_update
		this.version = 1
		this.getKV().store.issueUpdate(this)
	} else {
		this.errno = errcode.ERR_RECORD_EXIST
		this.reply()
		this.getKV().store.issueAddkv(this)
	}
}

func newAsynCmdTaskSetNx(cmd commandI) *asynCmdTaskSetNx {
	return &asynCmdTaskSetNx{
		asynCmdTaskBase: &asynCmdTaskBase{
			commands: []commandI{cmd},
		},
	}
}

type cmdSetNx struct {
	*commandBase
	fields map[string]*proto.Field
}

func (this *cmdSetNx) reply(errCode int32, fields map[string]*proto.Field, version int64) {
	this.replyer.reply(this, errCode, fields, version)
}

func (this *cmdSetNx) makeResponse(errCode int32, fields map[string]*proto.Field, version int64) *codec.Message {
	return codec.NewMessage(codec.CommonHead{
		Seqno:   this.replyer.seqno,
		ErrCode: errCode,
	}, &proto.SetNxResp{
		Version: version,
	})
}

func (this *cmdSetNx) prepare(t asynCmdTaskI) (asynCmdTaskI, bool) {

	if t != nil {
		return t, false
	}

	kv := this.kv
	status := kv.getStatus()

	if status == cache_ok {
		this.reply(errcode.ERR_RECORD_EXIST, nil, kv.version)
		return nil, true
	}

	task := newAsynCmdTaskSetNx(this)

	if status == cache_missing {
		task.fields = map[string]*proto.Field{}
		fillDefaultValue(kv.meta, &task.fields)
		task.sqlFlag = sql_insert_update
		for k, v := range this.fields {
			task.fields[k] = v
		}
		task.version = 1
	}

	return task, true
}

func setNx(n *KVNode, cli *cliConn, msg *codec.Message) {

	req := msg.GetData().(*proto.SetNxReq)

	head := msg.GetHead()

	processDeadline, respDeadline := getDeadline(head.Timeout)

	op := &cmdSetNx{
		commandBase: &commandBase{
			deadline: processDeadline,
			replyer:  newReplyer(cli, msg.GetHead().Seqno, respDeadline),
			version:  req.Version,
		},
		fields: map[string]*proto.Field{},
	}

	if len(req.GetFields()) == 0 {
		op.reply(errcode.ERR_MISSING_FIELDS, nil, 0)
		return
	}

	var err int32

	var kv *kv

	table, key := head.SplitUniKey()

	if kv, err = n.storeMgr.getkv(table, key, head.UniKey); errcode.ERR_OK != err {
		op.reply(err, nil, 0)
		return
	}

	op.kv = kv

	for _, v := range req.GetFields() {
		op.fields[v.GetName()] = v
	}

	if !kv.meta.CheckSet(op.fields) {
		op.reply(errcode.ERR_INVAILD_FIELD, nil, 0)
		return
	}

	kv.processCmd(op)

}
