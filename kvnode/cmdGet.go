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

type cmdGet struct {
	*commandBase
	fields map[string]*proto.Field
}

func (this *cmdGet) reply(errCode int32, fields map[string]*proto.Field, version int64) {
	this.replyer.reply(this, errCode, fields, version)
}

func (this *cmdGet) makeResponse(errCode int32, fields map[string]*proto.Field, version int64) pb.Message {

	var key string

	if nil != this.kv {
		key = this.kv.key
	}

	resp := &proto.GetResp{
		Head: &proto.RespCommon{
			Key:     pb.String(key),
			Seqno:   pb.Int64(this.replyer.seqno),
			ErrCode: pb.Int32(errCode),
			Version: pb.Int64(version),
		},
	}

	if errcode.ERR_OK == errCode {
		for _, field := range this.fields {
			v := fields[field.GetName()]
			if nil != v {
				resp.Fields = append(resp.Fields, v)
			}
		}
	}

	return resp
}

func get(n *kvnode, cli *cliConn, msg *codec.Message) {
	req := msg.GetData().(*proto.GetReq)
	head := req.GetHead()
	op := &cmdGet{
		commandBase: &commandBase{
			deadline: time.Now().Add(time.Duration(head.GetTimeout())),
			replyer:  newReplyer(cli, head.GetSeqno(), time.Now().Add(time.Duration(head.GetRespTimeout()))),
		},
		fields: map[string]*proto.Field{},
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

	if req.GetAll() {
		for _, name := range op.kv.meta.GetQueryMeta().GetFieldNames() {
			if name != "__key__" && name != "__version__" {
				op.fields[name] = proto.PackField(name, nil)
			}
		}
	} else {
		for _, name := range req.GetFields() {
			op.fields[name] = proto.PackField(name, nil)
		}
	}

	if err := kv.meta.CheckGet(op.fields); nil != err {
		op.reply(errcode.ERR_INVAILD_FIELD, nil, -1)
		return
	}

	if !kv.appendCmd(op) {
		op.reply(errcode.ERR_BUSY, nil, -1)
		return
	}

	kv.processQueueCmd()

}
