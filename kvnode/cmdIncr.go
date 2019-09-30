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

type cmdIncr struct {
	*commandBase
	incr *proto.Field
}

func (this *cmdIncr) reply(errCode int32, fields map[string]*proto.Field, version int64) {
	this.replyer.reply(this, errCode, fields, version)
}

func (this *cmdIncr) makeResponse(errCode int32, fields map[string]*proto.Field, version int64) pb.Message {

	var key string

	if nil != this.kv {
		key = this.kv.key
	}

	resp := &proto.IncrByResp{
		Head: &proto.RespCommon{
			Key:     pb.String(key),
			Seqno:   pb.Int64(this.replyer.seqno),
			ErrCode: pb.Int32(errCode),
			Version: pb.Int64(version),
		},
	}

	if errCode == errcode.ERR_OK {
		resp.NewValue = fields[this.incr.GetName()]
	}

	return resp
}

func incrBy(n *KVNode, cli *cliConn, msg *codec.Message) {

	req := msg.GetData().(*proto.IncrByReq)

	head := req.GetHead()

	op := &cmdIncr{
		commandBase: &commandBase{
			deadline: time.Now().Add(time.Duration(head.GetTimeout())),
			replyer:  newReplyer(cli, head.GetSeqno(), time.Now().Add(time.Duration(head.GetRespTimeout()))),
			version:  head.Version,
		},
		incr: req.GetField(),
	}

	err := checkReqCommon(head)

	if err != errcode.ERR_OK {
		op.reply(err, nil, 0)
		return
	}

	if nil == op.incr {
		op.reply(errcode.ERR_MISSING_FIELDS, nil, 0)
		return
	}

	kv, _ := n.storeMgr.getkv(head.GetTable(), head.GetKey())

	if nil == kv {
		op.reply(errcode.ERR_INVAILD_TABLE, nil, 0)
		return
	}

	op.kv = kv

	if err := kv.meta.CheckField(op.incr); nil != err {
		op.reply(errcode.ERR_INVAILD_FIELD, nil, 0)
		return
	}

	if !kv.appendCmd(op) {
		op.reply(errcode.ERR_BUSY, nil, 0)
		return
	}

	kv.processQueueCmd()

}
