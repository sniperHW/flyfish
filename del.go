package flyfish

import (
	"fmt"
	codec "flyfish/codec"
	protocol "flyfish/proto"
	"flyfish/errcode"
	"github.com/sniperHW/kendynet"
	"github.com/golang/protobuf/proto"
	"time"
)


type DelReplyer struct {
	seqno      int64
	session    kendynet.StreamSession
	cmd       *command
}

func (this *DelReplyer) reply(errCode int32,fields map[string]*protocol.Field,version int64) {
	
	if time.Now().After(this.cmd.deadline) {
		//已经超时
		return
	}

	resp := &protocol.DelResp{
		Seqno : proto.Int64(this.seqno),
		ErrCode : proto.Int32(errCode),
	}

	//Debugln("DelReply",this.context.uniKey,resp)	

	err := this.session.Send(resp)
	if nil != err {
		//记录日志
	}
}


func del(session kendynet.StreamSession,msg *codec.Message) {
	
	req := msg.GetData().(*protocol.DelReq)

	Debugln("del",req)

	errno := errcode.ERR_OK

	if "" == req.GetTable() {
		errno = errcode.ERR_MISSING_TABLE
	}

	if "" == req.GetKey() {
		errno = errcode.ERR_MISSING_KEY
	}

	if 0 != errno {
		resp := &protocol.DelResp{
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
		cmdType   : cmdDel,
		key       : req.GetKey(),
		table     : req.GetTable(),
		uniKey    : fmt.Sprintf("%s:%s",req.GetTable(),req.GetKey()),
		version   : req.Version,
		deadline  : time.Now().Add(time.Duration(req.GetTimeout())),
	}

	cmd.rpyer = &DelReplyer{
		seqno : req.GetSeqno(),
		session : session,
		cmd : cmd,		
	}
	
	pushCommand(cmd)
}