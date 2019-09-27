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

type opSetNx struct {
	kv       *kv
	deadline time.Time
	replyer  *replyer
	seqno    int64
	fields   map[string]*proto.Field
}

func (this *opSetNx) reply(errCode int32, fields map[string]*proto.Field, version int64) {
	this.replyer.reply(this, errCode, fields, version)
}

func (this *opSetNx) dontReply() {
	this.replyer.dontReply()
}

func (this *opSetNx) causeWriteBack() bool {
	return true
}

func (this *opSetNx) isSetOp() bool {
	return true
}

func (this *opSetNx) isReplyerClosed() bool {
	this.replyer.isClosed()
}

func (this *opSetNx) getKV() *kv {
	return this.kv
}

func (this *opSetNx) isTimeout() bool {
	return time.Now().After(this.deadline)
}

func (this *opSetNx) makeResponse(errCode int32, fields map[string]*proto.Field, version int64) pb.Message {

	var key string

	if nil != this.kv {
		key = this.kv.key
	}

	return &proto.SetNxResp{
		Head: &proto.RespCommon{
			Key:     pb.String(key),
			Seqno:   pb.Int64(this.seqno),
			ErrCode: pb.Int32(errCode),
			Version: pb.Int64(version),
		},
	}
}

func setNx(n *kvnode, session kendynet.StreamSession, msg *codec.Message) {

	req := msg.GetData().(*proto.SetNxReq)

	head := req.GetHead()

	head := req.GetHead()
	op := &opSetNx{
		deadline: time.Now().Add(time.Duration(head.GetTimeout())),
		replyer:  newReplyer(session, time.Now().Add(time.Duration(head.GetRespTimeout()))),
		seqno:    head.GetSeqno(),
		fields:   map[string]*proto.Field{},
	}

	err := checkReqCommon(head)

	if err != errcode.ERR_OK {
		op.reply(err, nil, -1)
		return
	}

	if len(req.GetFields()) == 0 {
		op.reply(errcode.ERR_MISSING_FIELDS, nil, -1)
		return
	}

	kv, _ := n.storeMgr.getkv(head.GetTable(), head.GetKey())

	if nil == kv {
		op.reply(errcode.ERR_INVAILD_TABLE, nil, -1)
		return
	}

	op.kv = kv

	for _, v := range req.GetFields() {
		op.fields[v.GetName()] = v
	}

	if !kv.meta.CheckSet(op.fields) {
		op.reply(errcode.ERR_INVAILD_FIELD, nil, -1)
		return
	}

	if !kv.opQueue.append(op) {
		op.reply(errcode.ERR_BUSY, nil, -1)
		return
	}

	kv.processQueueOp()

}
