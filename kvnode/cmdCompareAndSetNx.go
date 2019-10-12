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

type asynCmdTaskCompareAndSetNx struct {
	*asynCmdTaskBase
}

func (this *asynCmdTaskCompareAndSetNx) onSqlResp(errno int32) {
	this.asynCmdTaskBase.onSqlResp(errno)
	cmd := this.commands[0].(*cmdCompareAndSetNx)
	if errno == errcode.ERR_RECORD_NOTEXIST {
		this.fields = map[string]*proto.Field{}
		fillDefaultValue(cmd.getKV().meta, &this.fields)
		this.sqlFlag = sql_insert_update
		this.fields[cmd.newV.GetName()] = cmd.newV
		this.getKV().slot.issueUpdate(this)
	} else if errno == errcode.ERR_OK {
		if !cmd.checkVersion(this.version) {
			this.errno = errcode.ERR_VERSION_MISMATCH
		} else if !this.fields[cmd.oldV.GetName()].Equal(cmd.oldV) {
			this.errno = errcode.ERR_CAS_NOT_EQUAL
		} else {
			this.fields[cmd.oldV.GetName()] = cmd.newV
			this.version++
		}

		if this.errno != errcode.ERR_OK {
			this.reply()
			this.getKV().slot.issueAddkv(this)
		} else {
			this.sqlFlag = sql_update
			this.getKV().slot.issueUpdate(this)
		}

	}
}

func newAsynCmdTaskCompareAndSetNx(cmd commandI) *asynCmdTaskCompareAndSetNx {
	return &asynCmdTaskCompareAndSetNx{
		asynCmdTaskBase: &asynCmdTaskBase{
			commands: []commandI{cmd},
		},
	}
}

type cmdCompareAndSetNx struct {
	*commandBase
	oldV *proto.Field
	newV *proto.Field
}

func (this *cmdCompareAndSetNx) reply(errCode int32, fields map[string]*proto.Field, version int64) {
	this.replyer.reply(this, errCode, fields, version)
}

func (this *cmdCompareAndSetNx) makeResponse(errCode int32, fields map[string]*proto.Field, version int64) pb.Message {

	var key string

	if nil != this.kv {
		key = this.kv.key
	}

	resp := &proto.CompareAndSetNxResp{
		Head: &proto.RespCommon{
			Key:     key,                //pb.String(key),
			Seqno:   this.replyer.seqno, //pb.Int64(this.replyer.seqno),
			ErrCode: errCode,            //pb.Int32(errCode),
			Version: version,            //pb.Int64(version),
		}}
	if nil != fields {
		resp.Value = fields[this.oldV.GetName()]
	}

	return resp
}

func (this *cmdCompareAndSetNx) prepare(_ asynCmdTaskI) asynCmdTaskI {

	kv := this.kv

	status := kv.getStatus()

	if status == cache_ok {
		if !this.checkVersion(kv.version) {
			this.reply(errcode.ERR_VERSION_MISMATCH, nil, kv.version)
			return nil
		}

		if !this.kv.fields[this.oldV.GetName()].Equal(this.oldV) {
			this.reply(errcode.ERR_CAS_NOT_EQUAL, nil, kv.version)
			return nil
		}
	}

	task := newAsynCmdTaskCompareAndSetNx(this)

	if status == cache_ok {
		task.sqlFlag = sql_update
		task.fields = map[string]*proto.Field{}
		task.fields[this.newV.GetName()] = this.newV
		task.version = kv.version + 1
	} else if status == cache_missing {
		task.version = 1
		task.sqlFlag = sql_insert_update
		task.fields = map[string]*proto.Field{}
		fillDefaultValue(this.getKV().meta, &task.fields)
		task.fields[this.newV.GetName()] = this.newV
	}
	return task
}

func compareAndSetNx(n *KVNode, cli *cliConn, msg *codec.Message) {

	req := msg.GetData().(*proto.CompareAndSetNxReq)

	head := req.GetHead()

	op := &cmdCompareAndSetNx{
		commandBase: &commandBase{
			deadline: time.Now().Add(time.Duration(head.GetTimeout())),
			replyer:  newReplyer(cli, head.GetSeqno(), time.Now().Add(time.Duration(head.GetRespTimeout()))),
			version:  head.Version,
		},
		oldV: req.GetOld(),
		newV: req.GetNew(),
	}

	err := checkReqCommon(head)

	if err != errcode.ERR_OK {
		op.reply(err, nil, 0)
		return
	}

	if nil == op.newV || nil == op.oldV {
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

	if err := kv.meta.CheckCompareAndSet(op.newV, op.oldV); nil != err {
		op.reply(errcode.ERR_INVAILD_FIELD, nil, 0)
		return
	}

	if err = kv.appendCmd(op); err != errcode.ERR_OK {
		op.reply(err, nil, 0)
		return
	}

	kv.processQueueCmd()

}
