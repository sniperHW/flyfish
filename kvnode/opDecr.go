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

type opDecr struct {
	*opBase
	decr *proto.Field
}

func (this *opDecr) reply(errCode int32, fields map[string]*proto.Field, version int64) {
	this.replyer.reply(this, errCode, fields, version)
}

func (this *opDecr) makeResponse(errCode int32, fields map[string]*proto.Field, version int64) pb.Message {

	var key string

	if nil != this.kv {
		key = this.kv.key
	}

	resp := &proto.DecrByResp{
		Head: head,
	}

	if errCode == errcode.ERR_OK {
		resp.NewValue = fields[this.decr.GetName()]
	}

	return resp
}

func decrBy(n *kvnode, session kendynet.StreamSession, msg *codec.Message) {

	req := msg.GetData().(*proto.DecrByReq)

	head := req.GetHead()

	head := req.GetHead()
	op := &opDecr{
		opBase: &opBase{
			deadline: time.Now().Add(time.Duration(head.GetTimeout())),
			replyer:  newReplyer(session, time.Now().Add(time.Duration(head.GetRespTimeout()))),
			seqno:    head.GetSeqno(),
		},
		decr: req.GetField(),
	}

	err := checkReqCommon(head)

	if err != errcode.ERR_OK {
		op.reply(err, nil, -1)
		return
	}

	if nil == op.decr {
		op.reply(errcode.ERR_MISSING_FIELDS, nil, -1)
		return
	}

	kv, _ := n.storeMgr.getkv(head.GetTable(), head.GetKey())

	if nil == kv {
		op.reply(errcode.ERR_INVAILD_TABLE, nil, -1)
		return
	}

	op.kv = kv

	if !kv.meta.CheckField(op.decr) {
		op.reply(errcode.ERR_INVAILD_FIELD, nil, -1)
		return
	}

	if !kv.opQueue.append(op) {
		op.reply(errcode.ERR_BUSY, nil, -1)
		return
	}

	kv.processQueueOp()

}
