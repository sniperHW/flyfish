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

type opSet struct {
	kv       *kv
	deadline time.Time
	replyer  *replyer
	seqno    int64
	fields   map[string]*proto.Field
	version  *int64
}

func (this *opSet) reply(errCode int32, fields map[string]*proto.Field, version int64) {
	this.replyer.reply(this, errCode, fields, version)
}

func (this *opSet) dontReply() {
	this.replyer.dontReply()
}

func (this *opSet) causeWriteBack() bool {
	return true
}

func (this *opSet) isSetOp() bool {
	return true
}

func (this *opSet) isReplyerClosed() bool {
	this.replyer.isClosed()
}

func (this *opSet) getKV() *kv {
	return this.kv
}

func (this *opSet) isTimeout() bool {
	return time.Now().After(this.deadline)
}

func (this *opSet) makeResponse(errCode int32, fields map[string]*proto.Field, version int64) pb.Message {

	var key string

	if nil != this.kv {
		key = this.kv.key
	}

	return &proto.SetResp{
		Head: &proto.RespCommon{
			Key:     pb.String(key),
			Seqno:   pb.Int64(this.seqno),
			ErrCode: pb.Int32(errCode),
			Version: pb.Int64(version),
		},
	}
}

func set(n *kvnode, session kendynet.StreamSession, msg *codec.Message) {

	req := msg.GetData().(*proto.SetReq)

	head := req.GetHead()

	head := req.GetHead()
	op := &opSet{
		deadline: time.Now().Add(time.Duration(head.GetTimeout())),
		replyer:  newReplyer(session, time.Now().Add(time.Duration(head.GetRespTimeout()))),
		seqno:    head.GetSeqno(),
		fields:   map[string]*proto.Field{},
		version:  req.Version,
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
