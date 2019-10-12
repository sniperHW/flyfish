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

type asynCmdTaskCompareAndSet struct {
	*asynCmdTaskBase
}

func (this *asynCmdTaskCompareAndSet) onSqlResp(errno int32) {
	this.asynCmdTaskBase.onSqlResp(errno)
	if errno == errcode.ERR_RECORD_NOTEXIST {
		this.reply()
		this.getKV().slot.issueAddkv(this)
	} else if errno == errcode.ERR_OK {
		cmd := this.commands[0].(*cmdCompareAndSet)
		if !cmd.checkVersion(this.version) {
			this.errno = errcode.ERR_VERSION_MISMATCH
		} else if !this.fields[cmd.oldV.GetName()].IsEqual(cmd.oldV) {
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

func newAsynCmdTaskCompareAndSet(cmd commandI) *asynCmdTaskCompareAndSet {
	return &asynCmdTaskCompareAndSet{
		asynCmdTaskBase: &asynCmdTaskBase{
			commands: []commandI{cmd},
		},
	}
}

type cmdCompareAndSet struct {
	*commandBase
	oldV *proto.Field
	newV *proto.Field
}

func (this *cmdCompareAndSet) reply(errCode int32, fields map[string]*proto.Field, version int64) {
	this.replyer.reply(this, errCode, fields, version)
}

func (this *cmdCompareAndSet) makeResponse(errCode int32, fields map[string]*proto.Field, version int64) pb.Message {

	var key string

	if nil != this.kv {
		key = this.kv.key
	}

	resp := &proto.CompareAndSetResp{
		Head: makeRespCommon(key, this.replyer.seqno, errCode, version),
	}
	//Head: &proto.RespCommon{
	//	Key:     key,                //pb.String(key),
	//	Seqno:   this.replyer.seqno, //pb.Int64(this.replyer.seqno),
	//	ErrCode: errCode,            //pb.Int32(errCode),
	//	Version: version,            //pb.Int64(version),
	//}}
	if nil != fields {
		resp.Value = fields[this.oldV.GetName()]
	}

	return resp
}

func (this *cmdCompareAndSet) prepare(_ asynCmdTaskI) asynCmdTaskI {

	kv := this.kv

	status := kv.getStatus()

	if status == cache_missing {
		this.reply(errcode.ERR_RECORD_NOTEXIST, nil, 0)
		return nil
	} else {

		var task *asynCmdTaskCompareAndSet

		if status == cache_ok {
			if !this.checkVersion(kv.version) {
				this.reply(errcode.ERR_VERSION_MISMATCH, nil, kv.version)
				return nil
			}

			if !this.kv.fields[this.oldV.GetName()].IsEqual(this.oldV) {
				this.reply(errcode.ERR_CAS_NOT_EQUAL, nil, kv.version)
				return nil
			}

			task = newAsynCmdTaskCompareAndSet(this)
			task.sqlFlag = sql_update
			task.fields = map[string]*proto.Field{}
			task.fields[this.newV.GetName()] = this.newV
			task.version = kv.version + 1
		} else {
			task = newAsynCmdTaskCompareAndSet(this)
		}

		return task
	}
}

func compareAndSet(n *KVNode, cli *cliConn, msg *codec.Message) {

	req := msg.GetData().(*proto.CompareAndSetReq)

	head := req.GetHead()

	op := &cmdCompareAndSet{
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
