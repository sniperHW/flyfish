package flyfish

import (
	"fmt"
	codec "flyfish/codec"
	message "flyfish/proto"
	"flyfish/errcode"
	"github.com/sniperHW/kendynet"
	"github.com/golang/protobuf/proto"
)

////////////SetReplyer
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

/////////////////////SetNxReplyer

type SetNxReplyer struct {
	seqno      int64
	session    kendynet.StreamSession
	context    *cmdContext
}

func (this *SetNxReplyer) reply(errCode int32,fields map[string]field,version ...int64) {
	resp := &message.SetnxResp{
		Seqno : proto.Int64(this.seqno),
		ErrCode : proto.Int32(errCode),
	}

	if len(version) > 0 {
		resp.Version = proto.Int64(version[0])
	}

	Debugln("SetNxReply",this.context.uniKey,resp)

	//Errorln("SetReply",this.context.uniKey)	

	err := this.session.Send(resp)
	if nil != err {
		//记录日志
	}

}


func setNx(session kendynet.StreamSession,msg *codec.Message) {
	
	req := msg.GetData().(*message.SetnxReq)

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
		resp := &message.SetnxResp {
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
		cmdType   : cmdSetNx,
		key       : req.GetKey(),
		table     : req.GetTable(),
		uniKey    : fmt.Sprintf("%s:%s",req.GetTable(),req.GetKey()),
		fields    : []field{},
	}

	context.rpyer = &SetNxReplyer{
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

/////////////CompareAndSetReplyer

type CompareAndSetReplyer struct {
	seqno      int64
	session    kendynet.StreamSession
	context    *cmdContext
}

func (this *CompareAndSetReplyer) reply(errCode int32,fields map[string]field,version ...int64) {
	resp := &message.CompareAndSetResp{
		Seqno : proto.Int64(this.seqno),
		ErrCode : proto.Int32(errCode),
	}

	if len(version) > 0 {
		resp.Version = proto.Int64(version[0])
	}

	if errCode == errcode.ERR_OK || errCode == errcode.ERR_NOT_EQUAL {
		f,ok := fields[this.context.newV.name]
		if ok {
			resp.Value = message.PackField(f.name,f.value)
		} 
	} 

	Debugln("CompareAndSetReplyer",this.context.uniKey,resp,fields)

	//Errorln("SetReply",this.context.uniKey)	

	err := this.session.Send(resp)
	if nil != err {
		//记录日志
	}

}


func compareAndSet(session kendynet.StreamSession,msg *codec.Message) {
	
	req := msg.GetData().(*message.CompareAndSetReq)

	//Debugln("set",req,len(req.GetFields()))

	errno := errcode.ERR_OK

	if "" == req.GetTable() {
		errno = errcode.ERR_CMD_MISSING_TABLE
	}

	if "" == req.GetKey() {
		errno = errcode.ERR_CMD_MISSING_KEY
	}

	if 0 != errno {
		resp := &message.CompareAndSetResp {
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
		cmdType   : cmdCompareAndSet,
		key       : req.GetKey(),
		table     : req.GetTable(),
		uniKey    : fmt.Sprintf("%s:%s",req.GetTable(),req.GetKey()),
		fields    : []field{},
		oldV 	  : field {
			name  : req.GetOld().GetName(),
			value : message.UnpackField(req.GetOld()),
		},
		newV 	  : field {
			name  : req.GetNew().GetName(),
			value : message.UnpackField(req.GetNew()),
		},		
	}

	context.rpyer = &CompareAndSetReplyer{
		seqno : req.GetSeqno(),
		session : session,
		context : context,
	}

	context.fields = append(context.fields,field{
		name  : req.GetNew().GetName(),
		value : message.UnpackField(req.GetNew()),
	})

	pushCmdContext(context)
}


/////////////////////CompareAndSetNxReplyer

type CompareAndSetNxReplyer struct {
	seqno      int64
	session    kendynet.StreamSession
	context    *cmdContext
}

func (this *CompareAndSetNxReplyer) reply(errCode int32,fields map[string]field,version ...int64) {
	resp := &message.CompareAndSetnxResp{
		Seqno : proto.Int64(this.seqno),
		ErrCode : proto.Int32(errCode),
	}

	if len(version) > 0 {
		resp.Version = proto.Int64(version[0])
	}

	if errCode == errcode.ERR_OK || errCode == errcode.ERR_NOT_EQUAL {
		f,ok := fields[this.context.newV.name]
		if ok {
			resp.Value = message.PackField(f.name,f.value)
		} 
	} 

	Debugln("CompareAndSetNxReplyer",this.context.uniKey,resp,fields)

	//Errorln("SetReply",this.context.uniKey)	

	err := this.session.Send(resp)
	if nil != err {
		//记录日志
	}

}


func compareAndSetNx(session kendynet.StreamSession,msg *codec.Message) {
	
	req := msg.GetData().(*message.CompareAndSetnxReq)

	//Debugln("set",req,len(req.GetFields()))

	errno := errcode.ERR_OK

	if "" == req.GetTable() {
		errno = errcode.ERR_CMD_MISSING_TABLE
	}

	if "" == req.GetKey() {
		errno = errcode.ERR_CMD_MISSING_KEY
	}

	if 0 != errno {
		resp := &message.CompareAndSetnxResp {
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
		cmdType   : cmdCompareAndSetNx,
		key       : req.GetKey(),
		table     : req.GetTable(),
		uniKey    : fmt.Sprintf("%s:%s",req.GetTable(),req.GetKey()),
		fields    : []field{},
		oldV 	  : field {
			name  : req.GetOld().GetName(),
			value : message.UnpackField(req.GetOld()),
		},
		newV 	  : field {
			name  : req.GetNew().GetName(),
			value : message.UnpackField(req.GetNew()),
		},		
	}

	context.rpyer = &CompareAndSetNxReplyer{
		seqno : req.GetSeqno(),
		session : session,
		context : context,
	}

	context.fields = append(context.fields,field{
		name  : req.GetNew().GetName(),
		value : message.UnpackField(req.GetNew()),
	})

	pushCmdContext(context)
}