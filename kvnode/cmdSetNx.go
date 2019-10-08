package kvnode

import (
	//"fmt"
	pb "github.com/golang/protobuf/proto"
	codec "github.com/sniperHW/flyfish/codec"
	//"github.com/sniperHW/flyfish/dbmeta"
	"github.com/sniperHW/flyfish/errcode"
	"github.com/sniperHW/flyfish/proto"
	//"github.com/sniperHW/kendynet"
	"time"
)

type asynCmdTaskSetNx struct {
	*asynCmdTaskBase
}

func (this *asynCmdTaskSetNx) onSqlResp(errno int32) {
	this.asynCmdTaskBase.onSqlResp(errno)
	cmd := this.commands[0].(*cmdSet)

	if errno == errcode.ERR_RECORD_NOTFOUND {
		fillDefaultValue(cmd.getKV().meta, &this.fields)
		for k, v := range cmd.fields {
			this.fields[k] = v
		}
		this.sqlFlag = sql_insert
		this.version = 1
	} else {
		this.errno = errcode.ERR_RECORD_EXIST
		this.reply()
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
		Head: &proto.RespCommon{
			Key:     pb.String(key),
			Seqno:   pb.Int64(this.replyer.seqno),
			ErrCode: pb.Int32(errCode),
			Version: pb.Int64(version),
		},
	}
}

func (this *cmdSet) prepare(_ asynCmdTaskI) asynCmdTaskI {

	kv := this.kv
	status := kv.getStatus()

	if status == cache_ok {
		this.reply(errcode.ERR_RECORD_EXIST, nil, kv.version)
		return nil
	}

	task := newAsynCmdTaskSetNx(this)

	if status == cache_missing {
		fillDefaultValue(kv.meta, &task.fields)
		task.sqlFlag = sql_insert
		for k, v := range this.fields {
			task.fields[k] = v
		}
		task.version++
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

	kv, _ := n.storeMgr.getkv(head.GetTable(), head.GetKey())

	if nil == kv {
		op.reply(errcode.ERR_INVAILD_TABLE, nil, 0)
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
