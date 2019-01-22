package flyfish

import (
	codec "flyfish/codec"
	"flyfish/errcode"
	"flyfish/proto"
	"fmt"
	pb "github.com/golang/protobuf/proto"
	"github.com/sniperHW/kendynet"
	"time"
)

type DelReplyer struct {
	seqno   int64
	session kendynet.StreamSession
	cmd     *command
}

func (this *DelReplyer) reply(errCode int32, fields map[string]*proto.Field, version int64) {

	if time.Now().After(this.cmd.deadline) {
		//已经超时
		return
	}

	resp := &proto.DelResp{
		Seqno:   pb.Int64(this.seqno),
		ErrCode: pb.Int32(errCode),
	}

	//Debugln("DelReply",this.context.uniKey,resp)

	err := this.session.Send(resp)
	if nil != err {
		//记录日志
	}
}

func del(session kendynet.StreamSession, msg *codec.Message) {

	req := msg.GetData().(*proto.DelReq)

	Debugln("del", req)

	errno := errcode.ERR_OK

	for {

		if isStop() {
			errno = errcode.ERR_SERVER_STOPED
			break
		}

		if "" == req.GetTable() {
			errno = errcode.ERR_MISSING_TABLE
			break
		}

		if "" == req.GetKey() {
			errno = errcode.ERR_MISSING_KEY
			break
		}
		break
	}

	if 0 != errno {
		resp := &proto.DelResp{
			Seqno:   pb.Int64(req.GetSeqno()),
			ErrCode: pb.Int32(errno),
			Version: pb.Int64(-1),
		}
		err := session.Send(resp)
		if nil != err {
			//记录日志
		}
		return
	}

	cmd := &command{
		cmdType:  cmdDel,
		key:      req.GetKey(),
		table:    req.GetTable(),
		uniKey:   fmt.Sprintf("%s:%s", req.GetTable(), req.GetKey()),
		version:  req.Version,
		deadline: time.Now().Add(time.Duration(req.GetTimeout())),
	}

	cmd.rpyer = &DelReplyer{
		seqno:   req.GetSeqno(),
		session: session,
		cmd:     cmd,
	}

	processCmd(cmd)
}
