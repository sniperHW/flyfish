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

////////////SetReplyer
type SetReplyer struct {
	seqno   int64
	session kendynet.StreamSession
	cmd     *command
}

func (this *SetReplyer) reply(errCode int32, fields map[string]*proto.Field, version int64) {

	if time.Now().After(this.cmd.deadline) {
		//已经超时
		return
	}

	var resp pb.Message
	cmdType := this.cmd.cmdType

	if cmdType == cmdSet {
		r := &proto.SetResp{
			Seqno:   pb.Int64(this.seqno),
			ErrCode: pb.Int32(errCode),
			Version: pb.Int64(version),
		}
		resp = r
	} else if cmdType == cmdSetNx {
		r := &proto.SetNxResp{
			Seqno:   pb.Int64(this.seqno),
			ErrCode: pb.Int32(errCode),
			Version: pb.Int64(version),
		}
		resp = r
	} else if cmdType == cmdCompareAndSet {
		r := &proto.CompareAndSetResp{
			Seqno:   pb.Int64(this.seqno),
			ErrCode: pb.Int32(errCode),
			Version: pb.Int64(version),
		}
		if nil != fields {
			r.Value = fields[this.cmd.cns.oldV.GetName()]
		}
		resp = r
	} else if cmdType == cmdCompareAndSetNx {
		r := &proto.CompareAndSetNxResp{
			Seqno:   pb.Int64(this.seqno),
			ErrCode: pb.Int32(errCode),
			Version: pb.Int64(version),
		}
		if nil != fields {
			r.Value = fields[this.cmd.cns.oldV.GetName()]
		}
		resp = r
	} else {
		Debugln("invaild cmdType", cmdType)
		return
	}

	Debugln("reply set", resp)

	err := this.session.Send(resp)
	if nil != err {
		//记录日志
	}

}

func set(session kendynet.StreamSession, msg *codec.Message) {

	req := msg.GetData().(*proto.SetReq)

	//Debugln("set",req,len(req.GetFields()))

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

	if 0 == len(req.GetFields()) {
		errno = errcode.ERR_MISSING_FIELDS
	}

	if 0 != errno {
		resp := &proto.SetResp{
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
		cmdType:  cmdSet,
		key:      req.GetKey(),
		table:    req.GetTable(),
		uniKey:   fmt.Sprintf("%s:%s", req.GetTable(), req.GetKey()),
		version:  req.Version,
		fields:   map[string]*proto.Field{},
		deadline: time.Now().Add(time.Duration(req.GetTimeout())),
	}

	cmd.rpyer = &SetReplyer{
		seqno:   req.GetSeqno(),
		session: session,
		cmd:     cmd,
	}

	for _, v := range req.GetFields() {
		cmd.fields[v.GetName()] = v
	}

	processCmd(cmd)
}

func setNx(session kendynet.StreamSession, msg *codec.Message) {

	req := msg.GetData().(*proto.SetNxReq)

	//Debugln("set",req,len(req.GetFields()))

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

	if 0 == len(req.GetFields()) {
		errno = errcode.ERR_MISSING_FIELDS
	}

	if 0 != errno {
		resp := &proto.SetNxResp{
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
		cmdType:  cmdSetNx,
		key:      req.GetKey(),
		table:    req.GetTable(),
		uniKey:   fmt.Sprintf("%s:%s", req.GetTable(), req.GetKey()),
		fields:   map[string]*proto.Field{},
		deadline: time.Now().Add(time.Duration(req.GetTimeout())),
	}

	cmd.rpyer = &SetReplyer{
		seqno:   req.GetSeqno(),
		session: session,
		cmd:     cmd,
	}

	for _, v := range req.GetFields() {
		cmd.fields[v.GetName()] = v
	}

	processCmd(cmd)
}

func compareAndSet(session kendynet.StreamSession, msg *codec.Message) {

	req := msg.GetData().(*proto.CompareAndSetReq)

	//Debugln("set",req,len(req.GetFields()))

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
		resp := &proto.CompareAndSetResp{
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
		cmdType: cmdCompareAndSet,
		key:     req.GetKey(),
		table:   req.GetTable(),
		uniKey:  fmt.Sprintf("%s:%s", req.GetTable(), req.GetKey()),
		fields:  map[string]*proto.Field{},
		cns: &cnsSt{
			oldV: req.GetOld(),
			newV: req.GetNew(),
		},
		deadline: time.Now().Add(time.Duration(req.GetTimeout())),
	}

	cmd.rpyer = &SetReplyer{
		seqno:   req.GetSeqno(),
		session: session,
		cmd:     cmd,
	}

	processCmd(cmd)
}

func compareAndSetNx(session kendynet.StreamSession, msg *codec.Message) {

	req := msg.GetData().(*proto.CompareAndSetNxReq)

	//Debugln("set",req,len(req.GetFields()))

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
		resp := &proto.CompareAndSetNxResp{
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
		cmdType: cmdCompareAndSetNx,
		key:     req.GetKey(),
		table:   req.GetTable(),
		uniKey:  fmt.Sprintf("%s:%s", req.GetTable(), req.GetKey()),
		fields:  map[string]*proto.Field{},
		cns: &cnsSt{
			oldV: req.GetOld(),
			newV: req.GetNew(),
		},
		deadline: time.Now().Add(time.Duration(req.GetTimeout())),
	}

	cmd.rpyer = &SetReplyer{
		seqno:   req.GetSeqno(),
		session: session,
		cmd:     cmd,
	}

	processCmd(cmd)
}
