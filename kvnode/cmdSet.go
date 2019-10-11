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

type asynCmdTaskSet struct {
	*asynCmdTaskBase
}

func (this *asynCmdTaskSet) onSqlResp(errno int32) {
	this.asynCmdTaskBase.onSqlResp(errno)
	cmd := this.commands[0].(*cmdSet)

	if !cmd.checkVersion(this.version) {
		this.errno = errcode.ERR_VERSION_MISMATCH
	} else {
		if errno == errcode.ERR_RECORD_NOTEXIST {
			this.fields = map[string]*proto.Field{}
			fillDefaultValue(cmd.getKV().meta, &this.fields)
			this.sqlFlag = sql_insert_update
		} else {
			this.sqlFlag = sql_update
		}

		for k, v := range cmd.fields {
			this.fields[k] = v
		}

		this.errno = errcode.ERR_OK
		this.version++
	}

	if this.errno != errcode.ERR_OK {
		this.reply()
		this.getKV().slot.issueAddkv(this)
	} else {
		this.getKV().slot.issueUpdate(this)
	}
}

func newAsynCmdTaskSet(cmd commandI) *asynCmdTaskSet {
	return &asynCmdTaskSet{
		asynCmdTaskBase: &asynCmdTaskBase{
			commands: []commandI{cmd},
		},
	}
}

type cmdSet struct {
	*commandBase
	fields map[string]*proto.Field
}

func (this *cmdSet) reply(errCode int32, fields map[string]*proto.Field, version int64) {
	Debugln("cmdSet reply")
	this.replyer.reply(this, errCode, fields, version)
}

func (this *cmdSet) makeResponse(errCode int32, fields map[string]*proto.Field, version int64) pb.Message {

	var key string

	if nil != this.kv {
		key = this.kv.key
	}

	return &proto.SetResp{
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

	if !this.checkVersion(kv.version) {
		this.reply(errcode.ERR_VERSION_MISMATCH, nil, kv.version)
		return nil
	}

	task := newAsynCmdTaskSet(this)

	if status == cache_missing {
		task.fields = map[string]*proto.Field{}
		fillDefaultValue(kv.meta, &task.fields)
		task.sqlFlag = sql_insert_update
	} else if status == cache_ok {
		task.sqlFlag = sql_update
	}

	if status != cache_new {
		task.fields = this.fields
		task.version = kv.version + 1
		Debugln("task version", task.version)
	}

	return task
}

func set(n *KVNode, cli *cliConn, msg *codec.Message) {

	req := msg.GetData().(*proto.SetReq)

	head := req.GetHead()

	op := &cmdSet{
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
