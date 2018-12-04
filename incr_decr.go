package flyfish

import (
	"fmt"
	codec "flyfish/codec"
	message "flyfish/proto"
	"flyfish/errcode"
	"github.com/sniperHW/kendynet"
	"github.com/golang/protobuf/proto"
)


type IncrByReplyer struct {
	seqno      int64
	session    kendynet.StreamSession
	context    *cmdContext
}

func (this *IncrByReplyer) reply(errCode int32,fields map[string]field,version ...int64) {
	resp := &message.IncrbyResp{
		Seqno : proto.Int64(this.seqno),
		ErrCode : proto.Int32(errCode),
	}

	

	if errcode.ERR_OK == errCode {
		for _,field := range(this.context.fields) {
			v := fields[field.name]
			resp.NewValue = message.PackField(field.name,v.value) 
			break
		}
	}

	Debugln("IncrByReply",this.context.uniKey,resp,this.context.fields,resp.NewValue)

	err := this.session.Send(resp)
	if nil != err {
		//记录日志
	}

}

type DecrByReplyer struct {
	seqno      int64
	session    kendynet.StreamSession
	context    *cmdContext
}

func (this *DecrByReplyer) reply(errCode int32,fields map[string]field,version ...int64) {
	resp := &message.DecrbyResp{
		Seqno : proto.Int64(this.seqno),
		ErrCode : proto.Int32(errCode),
	}

	if errcode.ERR_OK == errCode {
		for _,field := range(this.context.fields) {
			v := fields[field.name]
			resp.NewValue = message.PackField(field.name,v.value) 
			break
		}
	}

	Debugln("DecrByReply",this.context.uniKey,*resp)	

	err := this.session.Send(resp)
	if nil != err {
		//记录日志
	}
}


func incrBy(session kendynet.StreamSession,msg *codec.Message) {
	
	req := msg.GetData().(*message.IncrbyReq)

	errno := errcode.ERR_OK

	if "" == req.GetTable() {
		errno = errcode.ERR_CMD_MISSING_TABLE
	}

	if "" == req.GetKey() {
		errno = errcode.ERR_CMD_MISSING_KEY
	}

	if 0 != errno {
		resp := &message.IncrbyResp {
			Seqno : proto.Int64(req.GetSeqno()),
			ErrCode : proto.Int32(errno),
		}
		err := session.Send(resp)
		if nil != err {
			//记录日志
		}		
		return
	}


	context := &cmdContext{
		cmdType   : cmdIncrBy,
		key       : req.GetKey(),
		table     : req.GetTable(),
		uniKey    : fmt.Sprintf("%s:%s",req.GetTable(),req.GetKey()),
		fields    : []field{
			field{
				name : req.GetField().GetName(),
				value : message.UnpackField(req.GetField()),
			},
		},
	}

	//fmt.Println(*context)

	context.rpyer = &IncrByReplyer{
		seqno : req.GetSeqno(),
		session : session,
		context : context,		
	}
	
	pushCmdContext(context)
}


func decrBy(session kendynet.StreamSession,msg *codec.Message) {
	
	req := msg.GetData().(*message.DecrbyReq)

	errno := errcode.ERR_OK

	if "" == req.GetTable() {
		errno = errcode.ERR_CMD_MISSING_TABLE
	}

	if "" == req.GetKey() {
		errno = errcode.ERR_CMD_MISSING_KEY
	}

	if 0 != errno {
		resp := &message.DecrbyResp {
			Seqno : proto.Int64(req.GetSeqno()),
			ErrCode : proto.Int32(errno),
		}
		err := session.Send(resp)
		if nil != err {
			//记录日志
		}		
		return
	}


	context := &cmdContext{
		cmdType   : cmdDecrBy,
		key       : req.GetKey(),
		table     : req.GetTable(),
		uniKey    : fmt.Sprintf("%s:%s",req.GetTable(),req.GetKey()),
		fields    : []field{
			field{
				name : req.GetField().GetName(),
				value : message.UnpackField(req.GetField()),
			},
		},
	}

	context.rpyer = &DecrByReplyer{
		seqno : req.GetSeqno(),
		session : session,
		context : context,		
	}
	
	pushCmdContext(context)
}