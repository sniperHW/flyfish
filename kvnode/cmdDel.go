package kvnode

import (
	codec "github.com/sniperHW/flyfish/codec"
	"github.com/sniperHW/flyfish/errcode"
	"github.com/sniperHW/flyfish/proto"
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
		this.getKV().store.issueAddkv(this)
	} else if errno == errcode.ERR_OK {
		this.sqlFlag = sql_delete
		this.getKV().store.issueUpdate(this)
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
	Debugln("cmdDel.reply", errCode)
	this.replyer.reply(this, errCode, fields, version)
}

func (this *cmdDel) makeResponse(errCode int32, fields map[string]*proto.Field, version int64) *codec.Message {
	return codec.NewMessage("", codec.CommonHead{
		Seqno:   this.replyer.seqno,
		ErrCode: errCode,
	}, &proto.DelResp{Version: version})

}

func (this *cmdDel) prepare(_ asynCmdTaskI) asynCmdTaskI {

	status := this.kv.getStatus()

	Debugln("cmdDel", this.kv.uniKey, status, this.kv.version, this.version)

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
			replyer:  newReplyer(cli, msg.GetHead().Seqno, time.Now().Add(time.Duration(head.GetRespTimeout()))),
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
