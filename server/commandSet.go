package server

import (
	"fmt"
	codec "github.com/sniperHW/flyfish/codec"
	"github.com/sniperHW/flyfish/proto"
	"time"

	pb "github.com/golang/protobuf/proto"
	"github.com/sniperHW/kendynet"
)

////////////SetReplyer
type SetReplyer struct {
	seqno   int64
	session kendynet.StreamSession
	cmd     *command
}

func (this *SetReplyer) reply(errCode int32, fields map[string]*proto.Field, version int64) {

	if time.Now().After(this.cmd.respDeadline) {
		//已经超时
		Debugln("reply SetReplyer timeout", this.cmd.key)
		return
	}

	Debugln("reply SetReplyer", this.cmd.key)

	head := &proto.RespCommon{
		Key:     pb.String(this.cmd.key),
		Seqno:   pb.Int64(this.seqno),
		ErrCode: pb.Int32(errCode),
		Version: pb.Int64(version),
	}

	if this.cmd.cmdType == cmdSet {
		this.session.Send(&proto.SetResp{Head: head})
	} else if this.cmd.cmdType == cmdSetNx {
		this.session.Send(&proto.SetNxResp{Head: head})
	} else if this.cmd.cmdType == cmdCompareAndSet {
		resp := &proto.CompareAndSetResp{Head: head}
		if nil != fields {
			resp.Value = fields[this.cmd.cns.oldV.GetName()]
		}
		this.session.Send(resp)
	} else if this.cmd.cmdType == cmdCompareAndSetNx {
		resp := &proto.CompareAndSetNxResp{Head: head}
		if nil != fields {
			resp.Value = fields[this.cmd.cns.oldV.GetName()]
		}
		this.session.Send(resp)
	} else {
		Debugln("invaild cmdType", this.cmd.cmdType)
	}
}

func set(session kendynet.StreamSession, msg *codec.Message) {

	req := msg.GetData().(*proto.SetReq)

	head := req.GetHead()

	Debugln("set", req)

	var (
		ok    bool
		errno int32
	)

	ok, errno = checkSetReq(head, req.GetFields())

	if !ok {
		session.Send(&proto.SetResp{
			Head: &proto.RespCommon{
				Key:     pb.String(head.GetKey()),
				Seqno:   pb.Int64(head.GetSeqno()),
				ErrCode: pb.Int32(errno),
				Version: pb.Int64(-1),
			},
		})
	} else {

		cmd := &command{
			cmdType:      cmdSet,
			key:          head.GetKey(),
			table:        head.GetTable(),
			uniKey:       fmt.Sprintf("%s:%s", head.GetTable(), head.GetKey()),
			version:      req.Version,
			fields:       map[string]*proto.Field{},
			deadline:     time.Now().Add(time.Duration(head.GetTimeout())),
			respDeadline: time.Now().Add(time.Duration(head.GetRespTimeout())),
		}

		cmd.rpyer = &SetReplyer{
			seqno:   head.GetSeqno(),
			session: session,
			cmd:     cmd,
		}

		for _, v := range req.GetFields() {
			cmd.fields[v.GetName()] = v
		}

		cmd.process()
	}
}

func setNx(session kendynet.StreamSession, msg *codec.Message) {

	req := msg.GetData().(*proto.SetNxReq)

	head := req.GetHead()

	Debugln("setNx", req)

	var (
		ok    bool
		errno int32
	)

	ok, errno = checkSetReq(head, req.GetFields())

	if !ok {
		session.Send(&proto.SetNxResp{
			Head: &proto.RespCommon{
				Key:     pb.String(head.GetKey()),
				Seqno:   pb.Int64(head.GetSeqno()),
				ErrCode: pb.Int32(errno),
				Version: pb.Int64(-1),
			},
		})
	} else {

		cmd := &command{
			cmdType:      cmdSetNx,
			key:          head.GetKey(),
			table:        head.GetTable(),
			uniKey:       fmt.Sprintf("%s:%s", head.GetTable(), head.GetKey()),
			fields:       map[string]*proto.Field{},
			deadline:     time.Now().Add(time.Duration(head.GetTimeout())),
			respDeadline: time.Now().Add(time.Duration(head.GetRespTimeout())),
		}

		cmd.rpyer = &SetReplyer{
			seqno:   head.GetSeqno(),
			session: session,
			cmd:     cmd,
		}

		for _, v := range req.GetFields() {
			cmd.fields[v.GetName()] = v
		}

		cmd.process()
	}
}

func compareAndSet(session kendynet.StreamSession, msg *codec.Message) {

	req := msg.GetData().(*proto.CompareAndSetReq)

	head := req.GetHead()

	Debugln("compareAndSet", req)

	var (
		ok    bool
		errno int32
	)

	ok, errno = checkCmpSetReq(head, req.GetNew(), req.GetOld())

	if !ok {
		session.Send(&proto.CompareAndSetResp{
			Head: &proto.RespCommon{
				Key:     pb.String(head.GetKey()),
				Seqno:   pb.Int64(head.GetSeqno()),
				ErrCode: pb.Int32(errno),
				Version: pb.Int64(-1),
			},
		})
	} else {

		cmd := &command{
			cmdType: cmdCompareAndSet,
			key:     head.GetKey(),
			table:   head.GetTable(),
			uniKey:  fmt.Sprintf("%s:%s", head.GetTable(), head.GetKey()),
			cns: &cnsSt{
				oldV: req.GetOld(),
				newV: req.GetNew(),
			},
			deadline:     time.Now().Add(time.Duration(head.GetTimeout())),
			respDeadline: time.Now().Add(time.Duration(head.GetRespTimeout())),
		}

		cmd.rpyer = &SetReplyer{
			seqno:   head.GetSeqno(),
			session: session,
			cmd:     cmd,
		}

		cmd.process()
	}
}

func compareAndSetNx(session kendynet.StreamSession, msg *codec.Message) {

	req := msg.GetData().(*proto.CompareAndSetNxReq)

	head := req.GetHead()

	Debugln("compareAndSet", req)

	var (
		ok    bool
		errno int32
	)

	ok, errno = checkCmpSetReq(head, req.GetNew(), req.GetOld())

	if !ok {
		session.Send(&proto.CompareAndSetNxResp{
			Head: &proto.RespCommon{
				Key:     pb.String(head.GetKey()),
				Seqno:   pb.Int64(head.GetSeqno()),
				ErrCode: pb.Int32(errno),
				Version: pb.Int64(-1),
			},
		})
	} else {

		cmd := &command{
			cmdType: cmdCompareAndSetNx,
			key:     head.GetKey(),
			table:   head.GetTable(),
			uniKey:  fmt.Sprintf("%s:%s", head.GetTable(), head.GetKey()),
			cns: &cnsSt{
				oldV: req.GetOld(),
				newV: req.GetNew(),
			},
			deadline:     time.Now().Add(time.Duration(head.GetTimeout())),
			respDeadline: time.Now().Add(time.Duration(head.GetRespTimeout())),
		}

		cmd.rpyer = &SetReplyer{
			seqno:   head.GetSeqno(),
			session: session,
			cmd:     cmd,
		}

		cmd.process()
	}
}
