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

type cmdCompareAndSetNx struct {
	*commandBase
	oldV *proto.Field
	newV *proto.Field
}

func (this *cmdCompareAndSetNx) reply(errCode int32, fields map[string]*proto.Field, version int64) {
	this.replyer.reply(this, errCode, fields, version)
}

func (this *cmdCompareAndSetNx) makeResponse(errCode int32, fields map[string]*proto.Field, version int64) pb.Message {

	var key string

	if nil != this.kv {
		key = this.kv.key
	}

	resp := &proto.CompareAndSetNxResp{
		Head: &proto.RespCommon{
			Key:     pb.String(key),
			Seqno:   pb.Int64(this.replyer.seqno),
			ErrCode: pb.Int32(errCode),
			Version: pb.Int64(version),
		}}
	if nil != fields {
		resp.Value = fields[this.oldV.GetName()]
	}

	return resp
}

func compareAndSetNx(n *kvnode, cli *cliConn, msg *codec.Message) {

	req := msg.GetData().(*proto.CompareAndSetNxReq)

	head := req.GetHead()

	op := &cmdCompareAndSetNx{
		commandBase: &commandBase{
			deadline: time.Now().Add(time.Duration(head.GetTimeout())),
			replyer:  newReplyer(cli, head.GetSeqno(), time.Now().Add(time.Duration(head.GetRespTimeout()))),
			version:  head.Version,
		},
		oldV: req.GetOld(),
		newV: req.GetNew(),
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

	if err := kv.meta.CheckCompareAndSet(op.newV, op.oldV); nil != err {
		op.reply(errcode.ERR_INVAILD_FIELD, nil, -1)
		return
	}

	if !kv.cmdQueue.append(op) {
		op.reply(errcode.ERR_BUSY, nil, -1)
		return
	}

	kv.processQueueCmd()

}
