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

type GetReplyer struct {
	seqno   int64
	session kendynet.StreamSession
	cmd     *command
	getAll  bool
}

func (this *GetReplyer) reply(errCode int32, fields map[string]*protocol.Field, version int64) {

	if time.Now().After(this.cmd.deadline) {
		//已经超时
		return
	}

	var resp proto.Message

	if !this.getAll {

		r := &protocol.GetResp{
			Seqno:   proto.Int64(this.seqno),
			ErrCode: proto.Int32(errCode),
			Version: proto.Int64(version),
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

		r := &protocol.GetAllResp{
			Seqno:   proto.Int64(this.seqno),
			ErrCode: proto.Int32(errCode),
			Version: proto.Int64(version),
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
	req := msg.GetData().(*protocol.GetAllReq)
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
		resp := &protocol.GetAllResp{
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
		cmdType:  cmdGet,
		key:      req.GetKey(),
		table:    req.GetTable(),
		uniKey:   fmt.Sprintf("%s:%s", req.GetTable(), req.GetKey()),
		fields:   map[string]*protocol.Field{},
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
			cmd.fields[name] = protocol.PackField(name, nil)
		}
	}

	pushCommand(cmd)
}

func get(session kendynet.StreamSession, msg *codec.Message) {
	req := msg.GetData().(*protocol.GetReq)
	errno := errcode.ERR_OK

	Debugln("get", req)

	if "" == req.GetTable() {
		errno = errcode.ERR_MISSING_TABLE
	}

	if "" == req.GetKey() {
		errno = errcode.ERR_MISSING_KEY
	}

	if errcode.ERR_OK != errno {
		resp := &protocol.GetResp{
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
		cmdType:  cmdGet,
		key:      req.GetKey(),
		table:    req.GetTable(),
		uniKey:   fmt.Sprintf("%s:%s", req.GetTable(), req.GetKey()),
		fields:   map[string]*protocol.Field{},
		deadline: time.Now().Add(time.Duration(req.GetTimeout())),
	}

	cmd.rpyer = &GetReplyer{
		seqno:   req.GetSeqno(),
		session: session,
		cmd:     cmd,
		getAll:  false,
	}

	for _, name := range req.GetFields() {
		cmd.fields[name] = protocol.PackField(name, nil)
	}
	pushCommand(cmd)
}
