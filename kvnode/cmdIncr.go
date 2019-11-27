package kvnode

import (
	pb "github.com/golang/protobuf/proto"
	codec "github.com/sniperHW/flyfish/codec"
	"github.com/sniperHW/flyfish/errcode"
	"github.com/sniperHW/flyfish/proto"
	"time"
)

type asynCmdTaskIncr struct {
	*asynCmdTaskBase
}

func (this *asynCmdTaskIncr) onSqlResp(errno int32) {
	this.asynCmdTaskBase.onSqlResp(errno)
	cmd := this.commands[0].(*cmdIncr)

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

		this.errno = errcode.ERR_OK
		oldV := this.fields[cmd.incr.GetName()]
		var newV *proto.Field
		if oldV.IsInt() {
			newV = proto.PackField(oldV.GetName(), oldV.GetInt()+cmd.incr.GetInt())
		} else {
			newV = proto.PackField(oldV.GetName(), oldV.GetUint()+cmd.incr.GetUint())
		}
		this.fields[oldV.GetName()] = newV
		this.version++
	}

	if this.errno != errcode.ERR_OK {
		this.reply()
		this.getKV().store.issueAddkv(this)
	} else {
		this.getKV().store.issueUpdate(this)
	}
}

func newAsynCmdTaskIncr(cmd commandI) *asynCmdTaskIncr {
	return &asynCmdTaskIncr{
		asynCmdTaskBase: &asynCmdTaskBase{
			commands: []commandI{cmd},
		},
	}
}

type cmdIncr struct {
	*commandBase
	incr *proto.Field
}

func (this *cmdIncr) reply(errCode int32, fields map[string]*proto.Field, version int64) {
	this.replyer.reply(this, errCode, fields, version)
}

func (this *cmdIncr) makeResponse(errCode int32, fields map[string]*proto.Field, version int64) pb.Message {

	var key string

	if nil != this.kv {
		key = this.kv.key
	}

	resp := &proto.IncrByResp{
		Head: makeRespCommon(key, this.replyer.seqno, errCode, version),
	}

	if errCode == errcode.ERR_OK {
		resp.NewValue = fields[this.incr.GetName()]
	}

	return resp
}

func (this *cmdIncr) prepare(_ asynCmdTaskI) asynCmdTaskI {

	kv := this.kv
	status := kv.getStatus()

	if !this.checkVersion(kv.version) {
		this.reply(errcode.ERR_VERSION_MISMATCH, nil, kv.version)
		return nil
	}

	task := newAsynCmdTaskIncr(this)

	if status == cache_missing {
		task.fields = map[string]*proto.Field{}
		fillDefaultValue(kv.meta, &task.fields)
		task.sqlFlag = sql_insert_update
	} else if status == cache_ok {
		task.fields = map[string]*proto.Field{}
		task.sqlFlag = sql_update
	}

	if status != cache_new {
		oldV := kv.fields[this.incr.GetName()]
		if nil == oldV {
			/*
			 * 表格新增加了列，但未设置过，使用默认值
			 */
			v := kv.meta.GetDefaultV(this.incr.GetName())
			if nil == v {
				this.reply(errcode.ERR_INVAILD_FIELD, nil, kv.version)
				return nil
			} else {
				oldV = proto.PackField(this.incr.GetName(), kv.meta.GetDefaultV(this.incr.GetName()))
			}
		}

		var newV *proto.Field
		if oldV.IsInt() {
			newV = proto.PackField(oldV.GetName(), oldV.GetInt()+this.incr.GetInt())
		} else {
			newV = proto.PackField(oldV.GetName(), oldV.GetUint()+this.incr.GetUint())
		}
		task.fields[oldV.GetName()] = newV
		task.version = kv.version + 1
	}

	return task

}

func incrBy(n *KVNode, cli *cliConn, msg *codec.Message) {

	req := msg.GetData().(*proto.IncrByReq)

	head := req.GetHead()

	op := &cmdIncr{
		commandBase: &commandBase{
			deadline: time.Now().Add(time.Duration(head.GetTimeout())),
			replyer:  newReplyer(cli, head.GetSeqno(), time.Now().Add(time.Duration(head.GetRespTimeout()))),
			version:  head.Version,
		},
		incr: req.GetField(),
	}

	err := checkReqCommon(head)

	if err != errcode.ERR_OK {
		op.reply(err, nil, 0)
		return
	}

	if nil == op.incr {
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

	if err := kv.meta.CheckField(op.incr); nil != err {
		op.reply(errcode.ERR_INVAILD_FIELD, nil, 0)
		return
	}

	if err = kv.appendCmd(op); err != errcode.ERR_OK {
		op.reply(err, nil, 0)
		return
	}

	kv.processQueueCmd()

}
