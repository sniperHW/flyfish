package raft

import (
	"fmt"
	pb "github.com/golang/protobuf/proto"
	codec "github.com/sniperHW/flyfish/codec"
	"github.com/sniperHW/flyfish/errcode"
	"github.com/sniperHW/flyfish/proto"
	"github.com/sniperHW/kendynet"
	"time"
)

type GetReplyer struct {
	seqno   int64
	session kendynet.StreamSession
	cmd     *command
}

func (this *GetReplyer) reply(errCode int32, fields map[string]*proto.Field, version int64) {

	if time.Now().After(this.cmd.respDeadline) {
		//已经超时
		Debugln("reply get timeout", this.cmd.key)
		return
	}

	resp := &proto.GetResp{
		Head: &proto.RespCommon{
			Key:     pb.String(this.cmd.key),
			Seqno:   pb.Int64(this.seqno),
			ErrCode: pb.Int32(errCode),
			Version: pb.Int64(version),
		},
	}

	if errcode.ERR_OK == errCode {
		for _, field := range this.cmd.fields {
			v := fields[field.GetName()]
			if nil != v {
				resp.Fields = append(resp.Fields, v)
			}
		}
	}

	Debugln("reply get", this.cmd.key)

	this.session.Send(resp)

}

func get(session kendynet.StreamSession, msg *codec.Message) {
	req := msg.GetData().(*proto.GetReq)
	head := req.GetHead()

	Debugln("get", req)

	var (
		ok    bool
		errno int32
	)

	ok, errno = checkReq(head)

	if !ok {
		session.Send(&proto.GetResp{
			Head: &proto.RespCommon{
				Key:     pb.String(head.GetKey()),
				Seqno:   pb.Int64(head.GetSeqno()),
				ErrCode: pb.Int32(errno),
				Version: pb.Int64(-1),
			},
		})
	} else {

		cmd := &command{
			cmdType:      cmdGet,
			key:          head.GetKey(),
			table:        head.GetTable(),
			uniKey:       fmt.Sprintf("%s:%s", head.GetTable(), head.GetKey()),
			fields:       map[string]*proto.Field{},
			deadline:     time.Now().Add(time.Duration(head.GetTimeout())),
			respDeadline: time.Now().Add(time.Duration(head.GetRespTimeout())),
		}

		cmd.rpyer = &GetReplyer{
			seqno:   head.GetSeqno(),
			session: session,
			cmd:     cmd,
		}

		if req.GetAll() {
			meta := getMetaByTable(head.GetTable())
			for _, name := range meta.queryMeta.field_names {
				if name != "__key__" && name != "__version__" {
					cmd.fields[name] = proto.PackField(name, nil)
				}
			}
		} else {

			for _, name := range req.GetFields() {
				cmd.fields[name] = proto.PackField(name, nil)
			}
		}

		cmd.process()
	}
}
