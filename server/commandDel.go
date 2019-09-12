package server

import (
	"fmt"
	pb "github.com/golang/protobuf/proto"
	codec "github.com/sniperHW/flyfish/codec"
	"github.com/sniperHW/flyfish/proto"
	"github.com/sniperHW/kendynet"
	"time"
)

type DelReplyer struct {
	seqno   int64
	session kendynet.StreamSession
	cmd     *command
}

func (this *DelReplyer) reply(errCode int32, fields map[string]*proto.Field, version int64) {

	Debugln("reply del", this.cmd.key)

	if time.Now().After(this.cmd.respDeadline) {
		Debugln("reply del timeout", this.cmd.key)
		//已经超时
		return
	}

	this.session.Send(&proto.DelResp{
		Head: &proto.RespCommon{
			Key:     pb.String(this.cmd.key),
			Seqno:   pb.Int64(this.seqno),
			ErrCode: pb.Int32(errCode),
		},
	})
}

func del(server *Server, session kendynet.StreamSession, msg *codec.Message) {

	req := msg.GetData().(*proto.DelReq)

	head := req.GetHead()

	Debugln("del", req)

	var (
		ok    bool
		errno int32
	)

	ok, errno = checkReq(head)

	if !ok {
		session.Send(&proto.DelResp{
			Head: &proto.RespCommon{
				Key:     pb.String(head.GetKey()),
				Seqno:   pb.Int64(head.GetSeqno()),
				ErrCode: pb.Int32(errno),
				Version: pb.Int64(-1),
			},
		})
	} else {

		cmd := &command{
			cmdType:      cmdDel,
			key:          head.GetKey(),
			table:        head.GetTable(),
			uniKey:       fmt.Sprintf("%s:%s", head.GetTable(), head.GetKey()),
			version:      req.Version,
			deadline:     time.Now().Add(time.Duration(head.GetTimeout())),
			respDeadline: time.Now().Add(time.Duration(head.GetRespTimeout())),
			storeGroup:   server.storeGroup,
		}

		cmd.rpyer = &DelReplyer{
			seqno:   head.GetSeqno(),
			session: session,
			cmd:     cmd,
		}

		cmd.process()
	}
}
