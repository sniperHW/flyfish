package rpc

import (
	"sync"
	"sync/atomic"
	"fmt"
	"runtime"
	//"github.com/sniperHW/kendynet"	
	"github.com/sniperHW/kendynet/util"
	//"time"
	//"os"
)

type RPCReplyer struct {
	encoder RPCMessageEncoder
	channel RPCChannel
	req    *RPCRequest
	fired	int32      //防止重复Reply		
}

func (this *RPCReplyer) Reply(ret interface{},err error) {
	if this.req.NeedResp && atomic.CompareAndSwapInt32(&this.fired,0,1) {
		response := &RPCResponse{Seq:this.req.Seq,Ret:ret}
		this.reply(response)
	}
}

func (this *RPCReplyer) reply(response RPCMessage) {
	msg,err := this.encoder.Encode(response)
	if nil != err {
		Errorf(util.FormatFileLine("Encode rpc response error:%s\n",err.Error()))
		return
	}
	err = this.channel.SendResponse(msg)
	if nil != err {		
		Errorf(util.FormatFileLine("send rpc response to (%s) error:%s\n",this.channel.Name() , err.Error()))
	}
	//Errorf("reply %d\n",response.GetSeq())	
}

type RPCMethodHandler func (*RPCReplyer,interface{})

type RPCServer struct {
	encoder   		 RPCMessageEncoder
	decoder   		 RPCMessageDecoder
	methods   		 map[string]RPCMethodHandler
	mutexMethods     sync.Mutex
	lastSeq          uint64 
}

func (this *RPCServer) RegisterMethod(name string,method RPCMethodHandler) error {
	if name == "" {
		return fmt.Errorf("name == ''")
	}

	if nil == method {
		return fmt.Errorf("method == nil")		
	}

	defer func(){
		this.mutexMethods.Unlock()
	}()
	this.mutexMethods.Lock()

	_,ok := this.methods[name]
	if ok {
		return fmt.Errorf("duplicate method:%s",name)
	} 
	this.methods[name] = method
	return nil
}

func (this *RPCServer) UnRegisterMethod(name string) {
	defer func(){
		this.mutexMethods.Unlock()
	}()
	this.mutexMethods.Lock()
	delete(this.methods,name)	
}

func (this *RPCServer) callMethod(method RPCMethodHandler,replyer *RPCReplyer,arg interface{}) {
	defer func(){
		if r := recover(); r != nil {
			buf := make([]byte, 65535)
			l := runtime.Stack(buf, false)
			err := fmt.Errorf("%v: %s", r, buf[:l])
			Errorf(util.FormatFileLine("%s\n",err.Error()))
			replyer.reply(&RPCResponse{Seq:replyer.req.Seq,Err:err})
		}			
	}()
	method(replyer,arg)
	return
}

/* 
*   如果需要单线程处理,可以提供eventQueue
*/
func (this *RPCServer) OnRPCMessage(channel RPCChannel,message interface{}) {
	msg,err := this.decoder.Decode(message)
	if nil != err {
		Errorf(util.FormatFileLine("RPCServer rpc message from(%s) decode err:%s\n",channel.Name,err.Error()))
		return
	}

	switch msg.(type) {
		case *RPCRequest: {
			req := msg.(*RPCRequest)
			this.mutexMethods.Lock()
			method,ok := this.methods[req.Method]
			this.mutexMethods.Unlock()
			if !ok {
				err = fmt.Errorf("invaild method:%s",req.Method)
				Errorf(util.FormatFileLine("rpc request from(%s) invaild method %s\n",channel.Name(),req.Method))		
			}

			replyer := &RPCReplyer{encoder:this.encoder,channel:channel,req:req}
			if nil != err {
				replyer.reply(&RPCResponse{Seq:req.Seq,Err:err})
			} else {
				this.callMethod(method,replyer,req.Arg)
			}			
		}
	}

}

func NewRPCServer(decoder RPCMessageDecoder,encoder RPCMessageEncoder) (*RPCServer,error) {
	if nil == decoder {
		return nil,fmt.Errorf("decoder == nil")
	}

	if nil == encoder {
		return nil,fmt.Errorf("encoder == nil")
	}

	mgr := &RPCServer{decoder:decoder,encoder:encoder}
	mgr.methods = make(map[string]RPCMethodHandler)
	return mgr,nil
}