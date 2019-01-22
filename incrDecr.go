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

type IncrDecrByReplyer struct {
	seqno   int64
	session kendynet.StreamSession
	cmd     *command
}

func (this *IncrDecrByReplyer) reply(errCode int32, fields map[string]*proto.Field, version int64) {

	if time.Now().After(this.cmd.deadline) {
		//已经超时
		return
	}

	var resp pb.Message
	cmdType := this.cmd.cmdType
	if cmdType == cmdIncrBy {
		r := &proto.IncrByResp{
			Seqno:   pb.Int64(this.seqno),
			ErrCode: pb.Int32(errCode),
			Version: pb.Int64(version),
		}
		r.NewValue = fields[this.cmd.incrDecr.GetName()]
		resp = r
	} else {
		r := &proto.DecrByResp{
			Seqno:   pb.Int64(this.seqno),
			ErrCode: pb.Int32(errCode),
			Version: pb.Int64(version),
		}
		r.NewValue = fields[this.cmd.incrDecr.GetName()]
		resp = r
	}

	err := this.session.Send(resp)
	if nil != err {
		//记录日志
	}
}

func incrBy(session kendynet.StreamSession, msg *codec.Message) {

	req := msg.GetData().(*proto.IncrByReq)

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
		resp := &proto.IncrByResp{
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
		cmdType:  cmdIncrBy,
		key:      req.GetKey(),
		table:    req.GetTable(),
		uniKey:   fmt.Sprintf("%s:%s", req.GetTable(), req.GetKey()),
		incrDecr: req.GetField(),
		deadline: time.Now().Add(time.Duration(req.GetTimeout())),
	}

	cmd.rpyer = &IncrDecrByReplyer{
		seqno:   req.GetSeqno(),
		session: session,
		cmd:     cmd,
	}

	processCmd(cmd)
}

func decrBy(session kendynet.StreamSession, msg *codec.Message) {

	req := msg.GetData().(*proto.DecrByReq)

	errno := errcode.ERR_OK

	if "" == req.GetTable() {
		errno = errcode.ERR_MISSING_TABLE
	}

	if "" == req.GetKey() {
		errno = errcode.ERR_MISSING_KEY
	}

	if 0 != errno {
		resp := &proto.DecrByResp{
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
		cmdType:  cmdDecrBy,
		key:      req.GetKey(),
		table:    req.GetTable(),
		uniKey:   fmt.Sprintf("%s:%s", req.GetTable(), req.GetKey()),
		incrDecr: req.GetField(),
		deadline: time.Now().Add(time.Duration(req.GetTimeout())),
	}

	cmd.rpyer = &IncrDecrByReplyer{
		seqno:   req.GetSeqno(),
		session: session,
		cmd:     cmd,
	}
	processCmd(cmd)
}
