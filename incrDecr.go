package flyfish

import (
	codec "flyfish/codec"
	"flyfish/errcode"
	protocol "flyfish/proto"
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/sniperHW/kendynet"
	"time"
)

type IncrDecrByReplyer struct {
	seqno   int64
	session kendynet.StreamSession
	cmd     *command
}

func (this *IncrDecrByReplyer) reply(errCode int32, fields map[string]*protocol.Field, version int64) {

	if time.Now().After(this.cmd.deadline) {
		//已经超时
		return
	}

	var resp proto.Message
	cmdType := this.cmd.cmdType
	if cmdType == cmdIncrBy {
		r := &protocol.IncrByResp{
			Seqno:   proto.Int64(this.seqno),
			ErrCode: proto.Int32(errCode),
			Version: proto.Int64(version),
		}
		r.NewValue = fields[this.cmd.incrDecr.GetName()]
		resp = r
	} else {
		r := &protocol.DecrByResp{
			Seqno:   proto.Int64(this.seqno),
			ErrCode: proto.Int32(errCode),
			Version: proto.Int64(version),
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

	req := msg.GetData().(*protocol.IncrByReq)

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
		resp := &protocol.IncrByResp{
			Seqno:   proto.Int64(req.GetSeqno()),
			ErrCode: proto.Int32(errno),
			Version: proto.Int64(-1),
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

	pushCommand(cmd)
}

func decrBy(session kendynet.StreamSession, msg *codec.Message) {

	req := msg.GetData().(*protocol.DecrByReq)

	errno := errcode.ERR_OK

	if "" == req.GetTable() {
		errno = errcode.ERR_MISSING_TABLE
	}

	if "" == req.GetKey() {
		errno = errcode.ERR_MISSING_KEY
	}

	if 0 != errno {
		resp := &protocol.DecrByResp{
			Seqno:   proto.Int64(req.GetSeqno()),
			ErrCode: proto.Int32(errno),
			Version: proto.Int64(-1),
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
	pushCommand(cmd)
}
