package kvnode

import (
	"fmt"
	pb "github.com/golang/protobuf/proto"
	codec "github.com/sniperHW/flyfish/codec"
	"github.com/sniperHW/flyfish/dbmeta"
	"github.com/sniperHW/flyfish/errcode"
	"github.com/sniperHW/flyfish/proto"
	"github.com/sniperHW/kendynet"
	"time"
)

type opIncr struct {
	kv       *kv
	deadline time.Time
	replyer  *replyer
	seqno    int64
	incr     *proto.Field
}

func (this *opIncr) reply(errCode int32, fields map[string]*proto.Field, version int64) {
	this.replyer.reply(this, errCode, fields, version)
}

func (this *opIncr) dontReply() {
	this.replyer.dontReply()
}

func (this *opIncr) causeWriteBack() bool {
	return true
}

func (this *opIncr) isSetOp() bool {
	return true
}

func (this *opIncr) isReplyerClosed() bool {
	this.replyer.isClosed()
}

func (this *opIncr) getKV() *kv {
	return this.kv
}

func (this *opIncr) isTimeout() bool {
	return time.Now().After(this.deadline)
}

func (this *opIncr) makeResponse(errCode int32, fields map[string]*proto.Field, version int64) pb.Message {

	var key string

	if nil != this.kv {
		key = this.kv.key
	}

	resp := &proto.IncrByResp{
		Head: head,
	}

	if errCode == errcode.ERR_OK {
		resp.NewValue = fields[this.incr.GetName()]
	}

	return resp
}

func incrBy(n *kvnode, session kendynet.StreamSession, msg *codec.Message) {

	req := msg.GetData().(*proto.IncrByReq)

	head := req.GetHead()

	head := req.GetHead()
	op := &opIncr{
		deadline: time.Now().Add(time.Duration(head.GetTimeout())),
		replyer:  newReplyer(session, time.Now().Add(time.Duration(head.GetRespTimeout()))),
		seqno:    head.GetSeqno(),
		incr:     req.GetField(),
	}

	err := checkReqCommon(head)

	if err != errcode.ERR_OK {
		op.reply(err, nil, -1)
		return
	}

	if nil == op.incr {
		op.reply(errcode.ERR_MISSING_FIELDS, nil, -1)
		return
	}

	kv, _ := n.storeMgr.getkv(head.GetTable(), head.GetKey())

	if nil == kv {
		op.reply(errcode.ERR_INVAILD_TABLE, nil, -1)
		return
	}

	op.kv = kv

	if !kv.meta.CheckField(op.incr) {
		op.reply(errcode.ERR_INVAILD_FIELD, nil, -1)
		return
	}

	if !kv.opQueue.append(op) {
		op.reply(errcode.ERR_BUSY, nil, -1)
		return
	}

	kv.processQueueOp()

}
