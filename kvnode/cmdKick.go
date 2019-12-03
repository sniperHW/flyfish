package kvnode

import (
	codec "github.com/sniperHW/flyfish/codec"
	"github.com/sniperHW/flyfish/errcode"
	"github.com/sniperHW/flyfish/proto"
	"github.com/sniperHW/flyfish/util/str"
	"time"
)

type asynCmdTaskKick struct {
	*asynCmdTaskBase
}

func (this *asynCmdTaskKick) append2Str(s *str.Str) {
	appendProposal2Str(s, proposal_kick, this.getKV().uniKey)
}

func (this *asynCmdTaskKick) onSqlResp(errno int32) {
	Errorln("should not reach here", this.getKV().uniKey)
}

func (this *asynCmdTaskKick) done() {
	kv := this.getKV()
	Debugln("asynCmdTaskKick.done()", kv.uniKey)
	kv.setRemoveAndClearCmdQueue(errcode.ERR_RETRY)
	kv.store.removeKv(kv)
}

func newAsynCmdTaskKick(cmd commandI) *asynCmdTaskKick {
	return &asynCmdTaskKick{
		asynCmdTaskBase: &asynCmdTaskBase{
			commands: []commandI{cmd},
		},
	}
}

type cmdKick struct {
	*commandBase
}

func (this *cmdKick) reply(errCode int32, fields map[string]*proto.Field, version int64) {
	Debugln("cmdKick reply")
	this.replyer.reply(this, errCode, fields, version)
}

func (this *cmdKick) makeResponse(errCode int32, fields map[string]*proto.Field, version int64) *codec.Message {
	return codec.NewMessage("", codec.CommonHead{
		Seqno:   this.replyer.seqno,
		ErrCode: errCode,
	}, &proto.KickResp{})

}

func (this *cmdKick) prepare(_ asynCmdTaskI) asynCmdTaskI {

	kv := this.kv
	status := kv.getStatus()

	if !(status == cache_ok || status == cache_missing) {
		this.reply(errcode.ERR_OTHER, nil, 0)
		return nil
	}

	if kv.isWriteBack() {
		this.reply(errcode.ERR_RETRY, nil, 0)
		return nil
	}

	return newAsynCmdTaskKick(this)
}

func kick(n *KVNode, cli *cliConn, msg *codec.Message) {

	req := msg.GetData().(*proto.KickReq)

	head := req.GetHead()

	op := &cmdKick{
		commandBase: &commandBase{
			deadline: time.Now().Add(time.Duration(head.GetTimeout())),
			replyer:  newReplyer(cli, msg.GetHead().Seqno, time.Now().Add(time.Duration(head.GetRespTimeout()))),
		},
	}

	err := checkReqCommon(head)

	if err != errcode.ERR_OK {
		op.reply(err, nil, 0)
		return
	}

	var kv *kv

	kv = n.storeMgr.getkvOnly(head.GetTable(), head.GetKey())

	if nil == kv {
		op.reply(errcode.ERR_OK, nil, 0)
		return
	}

	op.kv = kv

	if err = kv.appendCmd(op); err != errcode.ERR_OK {
		op.reply(err, nil, 0)
		return
	}

	kv.processQueueCmd()

}
