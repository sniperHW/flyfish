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

type opCompareAndSet struct {
	*opBase
	oldV *proto.Field
	newV *proto.Field
}

func (this *opCompareAndSet) reply(errCode int32, fields map[string]*proto.Field, version int64) {
	this.replyer.reply(this, errCode, fields, version)
}

func (this *opCompareAndSet) makeResponse(errCode int32, fields map[string]*proto.Field, version int64) pb.Message {

	var key string

	if nil != this.kv {
		key = this.kv.key
	}

	resp := &proto.CompareAndSetResp{
		Head: &proto.RespCommon{
			Key:     pb.String(key),
			Seqno:   pb.Int64(this.seqno),
			ErrCode: pb.Int32(errCode),
			Version: pb.Int64(version),
		}}
	if nil != fields {
		resp.Value = fields[this.oldV.GetName()]
	}

	return resp
}

func compareAndSet(n *kvnode, session kendynet.StreamSession, msg *codec.Message) {

	req := msg.GetData().(*proto.CompareAndSetReq)

	head := req.GetHead()

	head := req.GetHead()
	op := &opCompareAndSet{
		opBase: &opBase{
			deadline: time.Now().Add(time.Duration(head.GetTimeout())),
			replyer:  newReplyer(session, time.Now().Add(time.Duration(head.GetRespTimeout()))),
			seqno:    head.GetSeqno(),
		},
		fields:  map[string]*proto.Field{},
		version: req.Version,
		oldV:    req.GetOld(),
		newV:    req.GetNew(),
	}

	err := checkReqCommon(head)

	if err != errcode.ERR_OK {
		op.reply(err, nil, -1)
		return
	}

	if nil == op.newV || nil == op.oldV {
		op.reply(errcode.ERR_MISSING_FIELDS, nil, -1)
		return
	}

	kv, _ := n.storeMgr.getkv(head.GetTable(), head.GetKey())

	if nil == kv {
		op.reply(errcode.ERR_INVAILD_TABLE, nil, -1)
		return
	}

	op.kv = kv

	if !kv.meta.CheckCompareAndSet(op.newV, op.oldV) {
		op.reply(errcode.ERR_INVAILD_FIELD, nil, -1)
		return
	}

	if !kv.opQueue.append(op) {
		op.reply(errcode.ERR_BUSY, nil, -1)
		return
	}

	kv.processQueueOp()

}
