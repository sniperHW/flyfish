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

type asynCmdTaskDel struct {
	*asynCmdTaskBase
}

func (this *asynCmdTaskDel) onSqlResp(errno int32) {
	this.asynCmdTaskBase.onSqlResp(errno)
	if errno == errcode.ERR_RECORD_NOTEXIST {
		this.reply()
		//向副本同步插入操作
		this.getKV().slot.issueAddkv(this)
	} else if errno == errcode.ERR_OK {
		this.sqlFlag = sql_delete
		this.getKV().slot.issueUpdate(this)
	}
}

func newAsynCmdTaskDel(cmd commandI, sqlFlag uint32) *asynCmdTaskDel {
	return &asynCmdTaskDel{
		asynCmdTaskBase: &asynCmdTaskBase{
			commands: []commandI{cmd},
			sqlFlag:  sqlFlag,
		},
	}
}

type cmdDel struct {
	*commandBase
}

func (this *cmdDel) reply(errCode int32, fields map[string]*proto.Field, version int64) {
	this.replyer.reply(this, errCode, fields, version)
}

func (this *cmdDel) makeResponse(errCode int32, fields map[string]*proto.Field, version int64) pb.Message {

	var key string

	if nil != this.kv {
		key = this.kv.key
	}

	return &proto.DelResp{
		Head: &proto.RespCommon{
			Key:     key,                //pb.String(key),
			Seqno:   this.replyer.seqno, //pb.Int64(this.replyer.seqno),
			ErrCode: errCode,            //pb.Int32(errCode),
			Version: version,            //pb.Int64(version),
		},
	}
}

func (this *cmdDel) prepare(_ asynCmdTaskI) asynCmdTaskI {

	status := this.kv.getStatus()

	if status == cache_missing {
		this.reply(errcode.ERR_RECORD_NOTEXIST, nil, 0)
		return nil
	} else {
		if status == cache_ok && !this.checkVersion(this.kv.version) {
			this.reply(errcode.ERR_VERSION_MISMATCH, nil, this.kv.version)
			return nil
		}

		return newAsynCmdTaskDel(this, sql_delete)
	}
}

func del(n *KVNode, cli *cliConn, msg *codec.Message) {

	req := msg.GetData().(*proto.DelReq)

	head := req.GetHead()

	op := &cmdDel{
		commandBase: &commandBase{
			deadline: time.Now().Add(time.Duration(head.GetTimeout())),
			replyer:  newReplyer(cli, head.GetSeqno(), time.Now().Add(time.Duration(head.GetRespTimeout()))),
			version:  head.Version,
		},
	}

	err := checkReqCommon(head)

	if err != errcode.ERR_OK {
		op.reply(err, nil, 0)
		return
	}

	var kv *kv

	kv, err = n.storeMgr.getkv(head.GetTable(), head.GetKey())

	if errcode.ERR_OK != err {
		op.reply(err, nil, 0)
		return
	}

	op.kv = kv

	if err = kv.appendCmd(op); err != errcode.ERR_OK {
		op.reply(err, nil, 0)
		return
	}

	kv.processQueueCmd()
}
