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
	if errno == errcode.ERR_RECORD_NOTFOUND {
		fillDefaultValue(cmd.getKV().meta, &this.fields)
		this.sqlFlag = sql_insert
		this.fields[cmd.newV.GetName()] = cmd.newV
	} else if errno == errcode.ERR_OK {
		if !cmd.checkVersion(this.version) {
			this.errno = errcode.ERR_VERSION_MISMATCH
		} else if !this.fields[cmd.oldV.GetName()].Equal(cmd.oldV) {
			this.errno = errcode.ERR_CAS_NOT_EQUAL
		} else {
			this.sqlFlag = sql_update
			this.fields[cmd.oldV.GetName()] = cmd.newV
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
			Key:     pb.String(key),
			Seqno:   pb.Int64(this.replyer.seqno),
			ErrCode: pb.Int32(errCode),
			Version: pb.Int64(version),
		}}
	if nil != fields {
		resp.Value = fields[this.oldV.GetName()]
	}

	return resp
}

func (this *cmdCompareAndSetNx) prepare(_ asynCmdTaskI) asynCmdTaskI {

	status := this.kv.getStatus()

	if status == cache_ok {
		if !this.checkVersion(kv.version) {
			this.reply(errcode.ERR_VERSION, nil, kv.version)
			return nil
		}

		if !this.kv.fields[this.oldV.GetName()].Equal(this.oldV) {
			this.reply(errcode.ERR_CAS_NOT_EQUAL, nil, kv.version)
			return nil
		}
	}

	var sqlFlag uint32

	task := newAsynCmdTaskCompareAndSetNx(this)

	if status == cache_ok {
		task.sqlFlag = sql_update
		task.fields = map[string]*proto.Field{}
		task.fields[this.newV.GetName()] = this.newV
	} else if status == cache_missing {
		task.sqlFlag = sql_insert
		task.fields = map[string]*proto.Field{}
		fillDefaultValue(this.getKV().meta, &task.fields)
		task.fields[cmd.newV.GetName()] = cmd.newV
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

	kv, _ := n.storeMgr.getkv(head.GetTable(), head.GetKey())

	if nil == kv {
		op.reply(errcode.ERR_INVAILD_TABLE, nil, 0)
		return
	}

	op.kv = kv

	if err := kv.meta.CheckCompareAndSet(op.newV, op.oldV); nil != err {
		op.reply(errcode.ERR_INVAILD_FIELD, nil, 0)
		return
	}

	if !kv.appendCmd(op) {
		op.reply(errcode.ERR_BUSY, nil, 0)
		return
	}

	kv.processQueueCmd()

}
