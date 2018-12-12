package flyfish

import (
	"fmt"
	codec "flyfish/codec"
	protocol "flyfish/proto"
	"flyfish/errcode"
	"github.com/sniperHW/kendynet"
	"github.com/golang/protobuf/proto"
)

type GetReplyer struct {
	seqno      int64
	session    kendynet.StreamSession
	cmd        *command
	getAll     bool
}

func (this *GetReplyer) reply(errCode int32,fields map[string]*protocol.Field,version int64) {

	var resp proto.Message

	if !this.getAll {

		r := &protocol.GetResp{
			Seqno : proto.Int64(this.seqno),
			ErrCode : proto.Int32(errCode),
			Version : proto.Int64(version),
		}

		if errcode.ERR_OK == errCode {
			for _,field := range(this.cmd.fields) {
				v := fields[field.GetName()]
				if nil != v {
					r.Fields = append(r.Fields,v)
				}
			}
		}
		Debugln("GetReply",this.cmd.uniKey,r)
		resp = r

	} else{

		r := &protocol.GetAllResp{
			Seqno : proto.Int64(this.seqno),
			ErrCode : proto.Int32(errCode),
			Version : proto.Int64(version),
		}

		if errcode.ERR_OK == errCode {
			for _,field := range(this.cmd.fields) {
				v := fields[field.GetName()]
				if nil != v {
					r.Fields = append(r.Fields,v)
				}
			}
		}
		Debugln("GetAllReply",this.cmd.uniKey,r)
		resp = r		

	}

	err := this.session.Send(resp)
	if nil != err {
		//记录日志
		Debugln("send GetReply error",this.cmd.uniKey,resp,err)
	}
}

func getAll(session kendynet.StreamSession,msg *codec.Message) {
	req := msg.GetData().(*protocol.GetAllReq)
	errno := errcode.ERR_OK

	Debugln("getAll",req)

	if "" == req.GetTable() {
		errno = errcode.ERR_MISSING_TABLE
	}

	if "" == req.GetKey() {
		errno = errcode.ERR_MISSING_KEY
	}	

	meta := getMetaByTable(req.GetTable())

	if nil == meta {
		errno = errcode.ERR_INVAILD_TABLE
	}

	if errcode.ERR_OK != errno {
		resp := &protocol.GetAllResp{
			Seqno : proto.Int64(req.GetSeqno()),
			ErrCode : proto.Int32(errno),
			Version : proto.Int64(-1),
		}
		err := session.Send(resp)
		if nil != err {
			//记录日志
		}				
		return
	}

	cmd := &command{
		cmdType   : cmdGet,
		key       : req.GetKey(),
		table     : req.GetTable(),
		uniKey    : fmt.Sprintf("%s:%s",req.GetTable(),req.GetKey()),
		fields    : map[string]*protocol.Field{},
	}

	cmd.rpyer = &GetReplyer{
		seqno : req.GetSeqno(),
		session : session,
		cmd : cmd,
		getAll : true,		
	}

	for _,name := range(meta.queryMeta.field_names) {
		if name != "__key__" && name != "__version__" {
			cmd.fields[name] = protocol.PackField(name,nil)
		}
	}

	pushCommand(cmd)
}

func get(session kendynet.StreamSession,msg *codec.Message) {
	req := msg.GetData().(*protocol.GetReq)
	errno := errcode.ERR_OK

	Debugln("get",req)

	if "" == req.GetTable() {
		errno = errcode.ERR_MISSING_TABLE
	}

	if "" == req.GetKey() {
		errno = errcode.ERR_MISSING_KEY
	}

	if errcode.ERR_OK != errno {
		resp := &protocol.GetResp{
			Seqno : proto.Int64(req.GetSeqno()),
			ErrCode : proto.Int32(errno),
			Version : proto.Int64(-1),
		}
		err := session.Send(resp)
		if nil != err {
			//记录日志
		}				
		return
	}
	
	cmd := &command{
		cmdType   : cmdGet,
		key       : req.GetKey(),
		table     : req.GetTable(),
		uniKey    : fmt.Sprintf("%s:%s",req.GetTable(),req.GetKey()),
		fields    : map[string]*protocol.Field{},
	}

	cmd.rpyer = &GetReplyer{
		seqno : req.GetSeqno(),
		session : session,
		cmd : cmd,
		getAll : false,		
	}

	for _,name := range(req.GetFields()) {
		cmd.fields[name] = protocol.PackField(name,nil) 
	}
	pushCommand(cmd)
}

