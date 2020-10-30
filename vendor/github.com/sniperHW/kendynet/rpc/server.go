package rpc

import (
	"fmt"
	"github.com/sniperHW/kendynet"
	"github.com/sniperHW/kendynet/util"
	"sync"
	"sync/atomic"
)

type RPCReplyer struct {
	encoder RPCMessageEncoder
	channel RPCChannel
	req     *RPCRequest
	fired   int32 //防止重复Reply
}

func (this *RPCReplyer) Reply(ret interface{}, err error) {
	if this.req.NeedResp && atomic.CompareAndSwapInt32(&this.fired, 0, 1) {
		response := &RPCResponse{Seq: this.req.Seq, Ret: ret, Err: err}
		this.reply(response)
	}
}

func (this *RPCReplyer) reply(response RPCMessage) {
	msg, err := this.encoder.Encode(response)
	if nil != err {
		kendynet.GetLogger().Errorf(util.FormatFileLine("Encode rpc response error:%s\n", err.Error()))
		return
	}
	err = this.channel.SendResponse(msg)
	if nil != err {
		kendynet.GetLogger().Errorf(util.FormatFileLine("send rpc response to (%s) error:%s\n", this.channel.Name(), err.Error()))
	}
}

func (this *RPCReplyer) GetChannel() RPCChannel {
	return this.channel
}

type RPCMethodHandler func(*RPCReplyer, interface{})

type RPCServer struct {
	sync.RWMutex
	encoder RPCMessageEncoder
	decoder RPCMessageDecoder
	methods map[string]RPCMethodHandler
	lastSeq uint64
}

func (this *RPCServer) RegisterMethod(name string, method RPCMethodHandler) error {
	if name == "" {
		panic("name == ''")
	}

	if nil == method {
		panic("method == nil")
	}

	defer this.Unlock()
	this.Lock()

	_, ok := this.methods[name]
	if ok {
		return fmt.Errorf("duplicate method:%s", name)
	}
	this.methods[name] = method
	return nil
}

func (this *RPCServer) UnRegisterMethod(name string) {
	defer this.Unlock()
	this.Lock()
	delete(this.methods, name)
}

func (this *RPCServer) callMethod(method RPCMethodHandler, replyer *RPCReplyer, arg interface{}) {
	if _, err := util.ProtectCall(method, replyer, arg); nil != err {
		kendynet.GetLogger().Errorln(err.Error())
		replyer.reply(&RPCResponse{Seq: replyer.req.Seq, Err: err})
	}
}

/*
*   如果需要单线程处理,可以提供eventQueue
 */
func (this *RPCServer) OnRPCMessage(channel RPCChannel, message interface{}) {
	msg, err := this.decoder.Decode(message)
	if nil != err {
		kendynet.GetLogger().Errorln(util.FormatFileLine("RPCServer rpc message from(%s) decode err:%s\n", channel.Name(), err.Error()))
		return
	}

	switch msg.(type) {
	case *RPCRequest:
		{
			req := msg.(*RPCRequest)
			this.RLock()
			method, ok := this.methods[req.Method]
			this.RUnlock()
			if !ok {
				err = fmt.Errorf("invaild method:%s", req.Method)
				kendynet.GetLogger().Errorf(util.FormatFileLine("rpc request from(%s) invaild method %s\n", channel.Name(), req.Method))
			}

			replyer := &RPCReplyer{encoder: this.encoder, channel: channel, req: req}
			if nil != err {
				replyer.reply(&RPCResponse{Seq: req.Seq, Err: err})
			} else {
				this.callMethod(method, replyer, req.Arg)
			}
		}
		break
	default:
		panic("RPCServer.OnRPCMessage() invaild msg type")
		break
	}

}

func NewRPCServer(decoder RPCMessageDecoder, encoder RPCMessageEncoder) *RPCServer {
	if nil == decoder {
		panic("decoder == nil")
	}

	if nil == encoder {
		panic("encoder == nil")
	}

	return &RPCServer{
		decoder: decoder,
		encoder: encoder,
		methods: map[string]RPCMethodHandler{},
	}

}
