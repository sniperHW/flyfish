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

type GetReplyer struct {
	seqno   int64
	session kendynet.StreamSession
	cmd     *command
	getAll  bool
}

func (this *GetReplyer) reply(errCode int32, fields map[string]*proto.Field, version int64) {

	if time.Now().After(this.cmd.deadline) {
		//已经超时
		return
	}

	var resp pb.Message

	if !this.getAll {

		r := &proto.GetResp{
			Seqno:   pb.Int64(this.seqno),
			ErrCode: pb.Int32(errCode),
			Version: pb.Int64(version),
		}

		if errcode.ERR_OK == errCode {
			for _, field := range this.cmd.fields {
				v := fields[field.GetName()]
				if nil != v {
					r.Fields = append(r.Fields, v)
				}
			}
		}
		Debugln("GetReply", this.cmd.uniKey, r)
		resp = r

	} else {

		r := &proto.GetAllResp{
			Seqno:   pb.Int64(this.seqno),
			ErrCode: pb.Int32(errCode),
			Version: pb.Int64(version),
		}

		if errcode.ERR_OK == errCode {
			for _, field := range this.cmd.fields {
				v := fields[field.GetName()]
				if nil != v {
					r.Fields = append(r.Fields, v)
				}
			}
		}
		Debugln("GetAllReply", this.cmd.uniKey, r)
		resp = r

	}

	err := this.session.Send(resp)
	if nil != err {
		//记录日志
		Debugln("send GetReply error", this.cmd.uniKey, resp, err)
	}
}

func getAll(session kendynet.StreamSession, msg *codec.Message) {
	req := msg.GetData().(*proto.GetAllReq)
	errno := errcode.ERR_OK

	Debugln("getAll", req)

	var meta *table_meta

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

		meta = getMetaByTable(req.GetTable())

		if nil == meta {
			errno = errcode.ERR_INVAILD_TABLE
			break
		}
		break
	}

	if errcode.ERR_OK != errno {
		resp := &proto.GetAllResp{
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
		cmdType:  cmdGet,
		key:      req.GetKey(),
		table:    req.GetTable(),
		uniKey:   fmt.Sprintf("%s:%s", req.GetTable(), req.GetKey()),
		fields:   map[string]*proto.Field{},
		deadline: time.Now().Add(time.Duration(req.GetTimeout())),
	}

	cmd.rpyer = &GetReplyer{
		seqno:   req.GetSeqno(),
		session: session,
		cmd:     cmd,
		getAll:  true,
	}

	for _, name := range meta.queryMeta.field_names {
		if name != "__key__" && name != "__version__" {
			cmd.fields[name] = proto.PackField(name, nil)
		}
	}

	processCmd(cmd)
}

func get(session kendynet.StreamSession, msg *codec.Message) {
	req := msg.GetData().(*proto.GetReq)
	errno := errcode.ERR_OK

	Debugln("get", req)

	if "" == req.GetTable() {
		errno = errcode.ERR_MISSING_TABLE
	}

	if "" == req.GetKey() {
		errno = errcode.ERR_MISSING_KEY
	}

	if errcode.ERR_OK != errno {
		resp := &proto.GetResp{
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
		cmdType:  cmdGet,
		key:      req.GetKey(),
		table:    req.GetTable(),
		uniKey:   fmt.Sprintf("%s:%s", req.GetTable(), req.GetKey()),
		fields:   map[string]*proto.Field{},
		deadline: time.Now().Add(time.Duration(req.GetTimeout())),
	}

	cmd.rpyer = &GetReplyer{
		seqno:   req.GetSeqno(),
		session: session,
		cmd:     cmd,
		getAll:  false,
	}

	for _, name := range req.GetFields() {
		cmd.fields[name] = proto.PackField(name, nil)
	}
	processCmd(cmd)
}
