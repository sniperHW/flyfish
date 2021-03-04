package kvnode

import (
	"fmt"
	"github.com/sniperHW/flyfish/errcode"
	flyfish_logger "github.com/sniperHW/flyfish/logger"
	"github.com/sniperHW/flyfish/net"
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
	panic(fmt.Sprintf("asynCmdTaskKick should not reach here k:%s", this.getKV().uniKey))
}

func (this *asynCmdTaskKick) done() {
	kv := this.getKV()
	flyfish_logger.GetSugar().Debugf("asynCmdTaskKick.done() unikey:%s", kv.uniKey)
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
	//logger.Debugln("cmdKick reply")
	this.replyer.reply(this, errCode, fields, version)
}

func (this *cmdKick) makeResponse(errCode int32, fields map[string]*proto.Field, version int64) *net.Message {
	return net.NewMessage(net.CommonHead{
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

	if status == cache_remove {
		this.reply(errcode.ERR_OK, nil, 0)
		return nil, true
	}

	if kv.isWriteBack() {
		this.reply(errcode.ERR_RETRY, nil, 0)
		return nil, true
	}

	return newAsynCmdTaskKick(this), true
}

func kick(n *KVNode, cli *cliConn, msg *net.Message) {

	head := msg.GetHead()

	processDeadline, respDeadline := getDeadline(head.Timeout)

	op := &cmdKick{
		commandBase: &commandBase{
			deadline: processDeadline,
			replyer:  newReplyer(cli, msg.GetHead().Seqno, respDeadline),
		},
	}

	table, key := head.SplitUniKey()

	if kv := n.storeMgr.getkvOnly(table, key, head.UniKey); nil == kv {
		op.reply(errcode.ERR_OK, nil, 0)
		return
	} else {

		op.kv = kv

		kv.processCmd(op)
	}

}
