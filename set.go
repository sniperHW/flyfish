package flyfish

import (
	"fmt"
	codec "flyfish/codec"
	message "flyfish/proto"
	"flyfish/errcode"
	"github.com/sniperHW/kendynet"
	"github.com/golang/protobuf/proto"
)


type SetReplyer struct {
	seqno      int64
	session    kendynet.StreamSession
	context    *cmdContext
}

func (this *SetReplyer) reply(errCode int32,fields map[string]field,version ...int64) {
	resp := &message.SetResp{
		Seqno : proto.Int64(this.seqno),
		ErrCode : proto.Int32(errCode),
	}

	if len(version) > 0 {
		resp.Version = proto.Int64(version[0])
	}

	Debugln("SetReply",this.context.uniKey,resp)

	//Errorln("SetReply",this.context.uniKey)	

	err := this.session.Send(resp)
	if nil != err {
		//记录日志
	}

}


func set(session kendynet.StreamSession,msg *codec.Message) {
	
	req := msg.GetData().(*message.SetReq)

	//Debugln("set",req,len(req.GetFields()))

	errno := errcode.ERR_OK

	if "" == req.GetTable() {
		errno = errcode.ERR_CMD_MISSING_TABLE
	}

	if "" == req.GetKey() {
		errno = errcode.ERR_CMD_MISSING_KEY
	}



	if 0 == len(req.GetFields()) {
		errno = errcode.ERR_CMD_MISSING_FIELDS
	}

	if 0 != errno {
		resp := &message.SetResp {
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
		cmdType   : cmdSet,
		key       : req.GetKey(),
		table     : req.GetTable(),
		uniKey    : fmt.Sprintf("%s:%s",req.GetTable(),req.GetKey()),
		version   : req.Version,
		fields    : []field{},
	}

	context.rpyer = &SetReplyer{
		seqno : req.GetSeqno(),
		session : session,
		context : context,		
	}

	for _,v := range(req.GetFields()) {
		context.fields = append(context.fields,field{
			name  : v.GetName(),
			value : message.UnpackField(v),
		})
	}	
	pushCmdContext(context)
}