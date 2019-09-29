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

type cmdSet struct {
	*commandBase
	fields map[string]*proto.Field
}

func (this *cmdSet) reply(errCode int32, fields map[string]*proto.Field, version int64) {
	this.replyer.reply(this, errCode, fields, version)
}

func (this *cmdSet) makeResponse(errCode int32, fields map[string]*proto.Field, version int64) pb.Message {

	var key string

	if nil != this.kv {
		key = this.kv.key
	}

	return &proto.SetResp{
		Head: &proto.RespCommon{
			Key:     pb.String(key),
			Seqno:   pb.Int64(this.replyer.seqno),
			ErrCode: pb.Int32(errCode),
			Version: pb.Int64(version),
		},
	}
}

func set(n *kvnode, cli *cliConn, msg *codec.Message) {

	req := msg.GetData().(*proto.SetReq)

	head := req.GetHead()

	op := &cmdSet{
		commandBase: &commandBase{
			deadline: time.Now().Add(time.Duration(head.GetTimeout())),
			replyer:  newReplyer(cli, head.GetSeqno(), time.Now().Add(time.Duration(head.GetRespTimeout()))),
			version:  head.Version,
		},
		fields: map[string]*proto.Field{},
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

	if err := kv.meta.CheckSet(op.fields); nil != err {
		op.reply(errcode.ERR_INVAILD_FIELD, nil, -1)
		return
	}

	if !kv.cmdQueue.append(op) {
		op.reply(errcode.ERR_BUSY, nil, -1)
		return
	}

	kv.processQueueCmd()

}
