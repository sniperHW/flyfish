package flyfish

import (
	codec "flyfish/codec"
	"flyfish/proto"
	"fmt"
	"time"

	pb "github.com/golang/protobuf/proto"
	"github.com/sniperHW/kendynet"
)

type IncrDecrByReplyer struct {
	seqno   int64
	session kendynet.StreamSession
	cmd     *command
}

func (this *IncrDecrByReplyer) reply(errCode int32, fields map[string]*proto.Field, version int64) {

	if time.Now().After(this.cmd.deadline) {
		//已经超时
		Debugln("reply IncrDecrByReplyer timeout", this.cmd.key)
		return
	}

	Debugln("reply IncrDecrByReplyer", this.cmd.key)

	head := &proto.RespCommon{
		Key:     pb.String(this.cmd.key),
		Seqno:   pb.Int64(this.seqno),
		ErrCode: pb.Int32(errCode),
		Version: pb.Int64(version),
	}

	if this.cmd.cmdType == cmdIncrBy {
		this.session.Send(&proto.IncrByResp{
			Head:     head,
			NewValue: fields[this.cmd.incrDecr.GetName()],
		})
	} else {
		this.session.Send(&proto.DecrByResp{
			Head:     head,
			NewValue: fields[this.cmd.incrDecr.GetName()],
		})
	}
}

func incrBy(session kendynet.StreamSession, msg *codec.Message) {

	req := msg.GetData().(*proto.IncrByReq)

	head := req.GetHead()

	Debugln("incrBy", req)

	var (
		ok    bool
		errno int32
	)

	ok, errno = checkIncrDecrReq(head, req.GetField())

	if !ok {
		session.Send(&proto.IncrByResp{
			Head: &proto.RespCommon{
				Key:     pb.String(head.GetKey()),
				Seqno:   pb.Int64(head.GetSeqno()),
				ErrCode: pb.Int32(errno),
				Version: pb.Int64(-1),
			},
		})
	} else {

		cmd := &command{
			cmdType:  cmdIncrBy,
			key:      head.GetKey(),
			table:    head.GetTable(),
			uniKey:   fmt.Sprintf("%s:%s", head.GetTable(), head.GetKey()),
			incrDecr: req.GetField(),
			deadline: time.Now().Add(time.Duration(head.GetTimeout())),
		}

		cmd.rpyer = &IncrDecrByReplyer{
			seqno:   head.GetSeqno(),
			session: session,
			cmd:     cmd,
		}

		processCmd(cmd)
	}
}

func decrBy(session kendynet.StreamSession, msg *codec.Message) {

	req := msg.GetData().(*proto.DecrByReq)

	head := req.GetHead()

	Debugln("decrBy", req)

	var (
		ok    bool
		errno int32
	)

	ok, errno = checkIncrDecrReq(head, req.GetField())

	if !ok {
		session.Send(&proto.DecrByResp{
			Head: &proto.RespCommon{
				Key:     pb.String(head.GetKey()),
				Seqno:   pb.Int64(head.GetSeqno()),
				ErrCode: pb.Int32(errno),
				Version: pb.Int64(-1),
			},
		})
	} else {

		cmd := &command{
			cmdType:  cmdDecrBy,
			key:      head.GetKey(),
			table:    head.GetTable(),
			uniKey:   fmt.Sprintf("%s:%s", head.GetTable(), head.GetKey()),
			incrDecr: req.GetField(),
			deadline: time.Now().Add(time.Duration(head.GetTimeout())),
		}

		cmd.rpyer = &IncrDecrByReplyer{
			seqno:   head.GetSeqno(),
			session: session,
			cmd:     cmd,
		}

		processCmd(cmd)
	}
}
