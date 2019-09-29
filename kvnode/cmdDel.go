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

type cmdDel struct {
	*commandBase
}

func (this *cmdDel) reply(errCode int32, fields map[string]*proto.Field, version int64) {
	this.replyer.reply(this, errCode, fields, version)
}

func (this *cmdDel) makeResponse(errCode int32, fields map[string]*proto.Field, version int64) pb.Message {

	var key string

	if nil != this.kv {
		key = this.kv.key
	}

	return &proto.DelResp{
		Head: &proto.RespCommon{
			Key:     pb.String(key),
			Seqno:   pb.Int64(this.replyer.seqno),
			ErrCode: pb.Int32(errCode),
			Version: pb.Int64(version),
		},
	}
}

func del(n *kvnode, cli *cliConn, msg *codec.Message) {

	req := msg.GetData().(*proto.DelReq)

	head := req.GetHead()

	op := &cmdDel{
		commandBase: &commandBase{
			deadline: time.Now().Add(time.Duration(head.GetTimeout())),
			replyer:  newReplyer(cli, head.GetSeqno(), time.Now().Add(time.Duration(head.GetRespTimeout()))),
			version:  head.Version,
		},
	}

	err := checkReqCommon(head)

	if err != errcode.ERR_OK {
		op.reply(err, nil, -1)
		return
	}

	kv, _ := n.storeMgr.getkv(head.GetTable(), head.GetKey())

	if nil == kv {
		op.reply(errcode.ERR_INVAILD_TABLE, nil, -1)
		return
	}

	op.kv = kv

	if !kv.cmdQueue.append(op) {
		op.reply(errcode.ERR_BUSY, nil, -1)
		return
	}

	kv.processQueueCmd()
}
