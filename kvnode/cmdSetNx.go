package kvnode

import (
	pb "github.com/golang/protobuf/proto"
	codec "github.com/sniperHW/flyfish/codec"
	"github.com/sniperHW/flyfish/errcode"
	"github.com/sniperHW/flyfish/proto"
	"time"
)

type asynCmdTaskSetNx struct {
	*asynCmdTaskBase
}

func (this *asynCmdTaskSetNx) onSqlResp(errno int32) {
	this.asynCmdTaskBase.onSqlResp(errno)
	cmd := this.commands[0].(*cmdSet)

	if errno == errcode.ERR_RECORD_NOTEXIST {
		this.fields = map[string]*proto.Field{}
		fillDefaultValue(cmd.getKV().meta, &this.fields)
		for k, v := range cmd.fields {
			this.fields[k] = v
		}
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

func (this *cmdSetNx) makeResponse(errCode int32, fields map[string]*proto.Field, version int64) pb.Message {

	var key string

	if nil != this.kv {
		key = this.kv.key
	}

	return &proto.SetNxResp{
		Head: makeRespCommon(key, this.replyer.seqno, errCode, version),
	}
}

func (this *cmdSetNx) prepare(_ asynCmdTaskI) asynCmdTaskI {

	kv := this.kv
	status := kv.getStatus()

	if status == cache_ok {
		this.reply(errcode.ERR_RECORD_EXIST, nil, kv.version)
		return nil
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

	return task
}

func setNx(n *KVNode, cli *cliConn, msg *codec.Message) {

	req := msg.GetData().(*proto.SetNxReq)

	head := req.GetHead()

	op := &cmdSetNx{
		commandBase: &commandBase{
			deadline: time.Now().Add(time.Duration(head.GetTimeout())),
			replyer:  newReplyer(cli, head.GetSeqno(), time.Now().Add(time.Duration(head.GetRespTimeout()))),
			version:  head.Version,
		},
		fields: map[string]*proto.Field{},
	}

	err := checkReqCommon(head)

	if err != errcode.ERR_OK {
		op.reply(err, nil, 0)
		return
	}

	if len(req.GetFields()) == 0 {
		op.reply(errcode.ERR_MISSING_FIELDS, nil, 0)
		return
	}

	var kv *kv

	kv, err = n.storeMgr.getkv(head.GetTable(), head.GetKey())

	if errcode.ERR_OK != err {
		op.reply(err, nil, 0)
		return
	}

	op.kv = kv

	for _, v := range req.GetFields() {
		op.fields[v.GetName()] = v
	}

	if err := kv.meta.CheckSet(op.fields); nil != err {
		op.reply(errcode.ERR_INVAILD_FIELD, nil, 0)
		return
	}

	if err = kv.appendCmd(op); err != errcode.ERR_OK {
		op.reply(err, nil, 0)
		return
	}

	kv.processQueueCmd()

}
