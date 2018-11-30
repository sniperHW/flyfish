package flyfish

import (
	"fmt"
	codec "flyfish/codec"
	message "flyfish/proto"
	"flyfish/errcode"
	"github.com/sniperHW/kendynet"
	"github.com/golang/protobuf/proto"
)


type DelReplyer struct {
	seqno      int64
	session    kendynet.StreamSession
	context    *cmdContext
}

func (this *DelReplyer) reply(errCode int32,fields map[string]field,version ...int64) {
	resp := &message.DelResp{
		Seqno : proto.Int64(this.seqno),
		ErrCode : proto.Int32(errCode),
	}

	Debugln("DelReply",this.context.uniKey,resp)	

	err := this.session.Send(resp)
	if nil != err {
		//记录日志
	}
}


func del(session kendynet.StreamSession,msg *codec.Message) {
	
	req := msg.GetData().(*message.DelReq)

	Debugln("del",req)

	errno := errcode.ERR_OK

	if "" == req.GetTable() {
		errno = errcode.ERR_CMD_MISSING_TABLE
	}

	if "" == req.GetKey() {
		errno = errcode.ERR_CMD_MISSING_KEY
	}

	if 0 != errno {
		resp := &message.DelResp{
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
		cmdType   : cmdDel,
		key       : req.GetKey(),
		table     : req.GetTable(),
		uniKey    : fmt.Sprintf("%s:%s",req.GetTable(),req.GetKey()),
		version   : req.Version,
	}

	context.rpyer = &DelReplyer{
		seqno : req.GetSeqno(),
		session : session,
		context : context,		
	}
	
	pushCmdContext(context)
}