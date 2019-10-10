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

type asynCmdTaskDecr struct {
	*asynCmdTaskBase
}

func (this *asynCmdTaskDecr) onSqlResp(errno int32) {
	this.asynCmdTaskBase.onSqlResp(errno)
	cmd := this.commands[0].(*cmdDecr)

	if !cmd.checkVersion(this.version) {
		this.errno = errcode.ERR_VERSION_MISMATCH
	} else {
		if errno == errcode.ERR_RECORD_NOTEXIST {
			this.fields = map[string]*proto.Field{}
			fillDefaultValue(cmd.getKV().meta, &this.fields)
			this.sqlFlag = sql_insert
		} else if errno == errcode.ERR_OK {
			this.sqlFlag = sql_update
		}
		this.errno = errcode.ERR_OK
		oldV := this.fields[cmd.decr.GetName()]
		var newV *proto.Field

		if oldV.IsInt() {
			newV = proto.PackField(oldV.GetName(), oldV.GetInt()-cmd.decr.GetInt())
		} else {
			newV = proto.PackField(oldV.GetName(), oldV.GetUint()-cmd.decr.GetUint())
		}

		this.fields[oldV.GetName()] = newV
		this.version++
	}

	if this.errno != errcode.ERR_OK {
		this.reply()
		this.getKV().slot.issueAddkv(this)
	} else {
		this.getKV().slot.issueUpdate(this)
	}
}

func newAsynCmdTaskDecr(cmd commandI) *asynCmdTaskDecr {
	return &asynCmdTaskDecr{
		asynCmdTaskBase: &asynCmdTaskBase{
			commands: []commandI{cmd},
		},
	}
}

type cmdDecr struct {
	*commandBase
	decr *proto.Field
}

func (this *cmdDecr) reply(errCode int32, fields map[string]*proto.Field, version int64) {
	this.replyer.reply(this, errCode, fields, version)
}

func (this *cmdDecr) makeResponse(errCode int32, fields map[string]*proto.Field, version int64) pb.Message {

	var key string

	if nil != this.kv {
		key = this.kv.key
	}

	resp := &proto.DecrByResp{
		Head: &proto.RespCommon{
			Key:     pb.String(key),
			Seqno:   pb.Int64(this.replyer.seqno),
			ErrCode: pb.Int32(errCode),
			Version: pb.Int64(version),
		},
	}

	if errCode == errcode.ERR_OK {
		resp.NewValue = fields[this.decr.GetName()]
	}

	return resp
}

func (this *cmdDecr) prepare(_ asynCmdTaskI) asynCmdTaskI {

	kv := this.kv
	status := kv.getStatus()

	if !this.checkVersion(kv.version) {
		this.reply(errcode.ERR_VERSION_MISMATCH, nil, kv.version)
		return nil
	}

	task := newAsynCmdTaskDecr(this)

	if status == cache_missing {
		task.fields = map[string]*proto.Field{}
		fillDefaultValue(kv.meta, &task.fields)
		task.sqlFlag = sql_insert
	} else if status == cache_ok {
		task.fields = map[string]*proto.Field{}
		task.sqlFlag = sql_update
	}

	if status != cache_new {
		oldV := kv.fields[this.decr.GetName()]
		var newV *proto.Field
		if oldV.IsInt() {
			newV = proto.PackField(oldV.GetName(), oldV.GetInt()-this.decr.GetInt())
		} else {
			newV = proto.PackField(oldV.GetName(), oldV.GetUint()-this.decr.GetUint())
		}
		task.fields[oldV.GetName()] = newV
		task.version++
	}

	return task

}

func decrBy(n *KVNode, cli *cliConn, msg *codec.Message) {

	req := msg.GetData().(*proto.DecrByReq)

	head := req.GetHead()

	op := &cmdDecr{
		commandBase: &commandBase{
			deadline: time.Now().Add(time.Duration(head.GetTimeout())),
			replyer:  newReplyer(cli, head.GetSeqno(), time.Now().Add(time.Duration(head.GetRespTimeout()))),
			version:  head.Version,
		},
		decr: req.GetField(),
	}

	err := checkReqCommon(head)

	if err != errcode.ERR_OK {
		op.reply(err, nil, 0)
		return
	}

	if nil == op.decr {
		op.reply(errcode.ERR_MISSING_FIELDS, nil, 0)
		return
	}

	kv, _ := n.storeMgr.getkv(head.GetTable(), head.GetKey())

	if nil == kv {
		op.reply(errcode.ERR_INVAILD_TABLE, nil, 0)
		return
	}

	op.kv = kv

	if err := kv.meta.CheckField(op.decr); nil != err {
		op.reply(errcode.ERR_INVAILD_FIELD, nil, 0)
		return
	}

	if err = kv.appendCmd(op); err != errcode.ERR_OK {
		op.reply(err, nil, 0)
		return
	}

	kv.processQueueCmd()

}
