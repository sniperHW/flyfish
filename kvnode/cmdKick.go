package kvnode

import (
	codec "github.com/sniperHW/flyfish/codec"
	"github.com/sniperHW/flyfish/errcode"
	"github.com/sniperHW/flyfish/proto"
	"github.com/sniperHW/flyfish/util/str"
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
	//kv.Lock()
	//kv.setRemoveAndClearCmdQueue(errcode.ERR_RETRY)
	//kv.Unlock()
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
	return codec.NewMessage(codec.CommonHead{
		Seqno:   this.replyer.seqno,
		ErrCode: errCode,
	}, &proto.KickResp{})

}

func (this *cmdKick) prepare(t asynCmdTaskI) (asynCmdTaskI, bool) {

	if t != nil {
		return t, false
	}

	kv := this.kv
	status := kv.getStatus()

	if !(status == cache_ok || status == cache_missing) {
		this.reply(errcode.ERR_OTHER, nil, 0)
		return nil, true
	}

	if kv.isWriteBack() {
		this.reply(errcode.ERR_RETRY, nil, 0)
		return nil, true
	}

	return newAsynCmdTaskKick(this), true
}

func kick(n *KVNode, cli *cliConn, msg *codec.Message) {

	head := msg.GetHead()

	processDeadline, respDeadline := getDeadline(head.Timeout)

	op := &cmdKick{
		commandBase: &commandBase{
			deadline: processDeadline,
			replyer:  newReplyer(cli, msg.GetHead().Seqno, respDeadline),
		},
	}

	var err int32

	var kv *kv

	table, key := head.SplitUniKey()

	if kv, err = n.storeMgr.getkv(table, key, head.UniKey); errcode.ERR_OK != err {
		op.reply(err, nil, 0)
		return
	}

	op.kv = kv

	kv.processCmd(op)

}
