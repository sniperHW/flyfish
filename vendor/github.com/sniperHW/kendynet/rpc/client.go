package rpc

import (
	"fmt"
	"sync/atomic"
	"sync"
	"time"
	"runtime"	
	"github.com/sniperHW/kendynet/event"
	"github.com/sniperHW/kendynet/util"	
	"github.com/sniperHW/kendynet"
	"os"
)

var ErrCallTimeout error = fmt.Errorf("rpc call timeout")

type RPCResponseHandler func(interface{},error)

type reqContext struct {
	heapIdx     uint32
	seq         uint64
	onResponse  RPCResponseHandler
	deadline    time.Time
	timestamp   int64 
}

func (this *reqContext) Less(o util.HeapElement) bool {
	return o.(*reqContext).deadline.After(this.deadline)
}

func (this *reqContext) GetIndex() uint32 {
	return this.heapIdx
}

func (this *reqContext) SetIndex(idx uint32) {
	this.heapIdx = idx
}

func (this *reqContext) callResponseCB(ret interface{},err error) {
	pCallOnResponse(this.onResponse,ret,err)
}

func pCallOnResponse(onResponse  RPCResponseHandler,ret interface{}, err error) {
	defer func(){
		if r := recover(); r != nil {
			buf := make([]byte, 65535)
			l := runtime.Stack(buf, false)
			err := fmt.Errorf("%v: %s", r, buf[:l])
			Errorf(util.FormatFileLine("%s\n",err.Error()))
		}			
	}()
	onResponse(ret,err)	
}

type channelContext struct {
	minheap          *util.MinHeap
	pendingCalls      map[uint64]*reqContext
}

type reqContextMgr struct {
	queue           *event.EventQueue //*util.BlockQueue   
	channels         map[RPCChannel]*channelContext
}

func (this *reqContextMgr) onRequest(channel RPCChannel,context *reqContext,msg interface{}) {
	var ok bool
	var cContext *channelContext
	err := channel.SendRequest(msg)
	if nil != err {
		context.callResponseCB(nil,err)
	} else {
		if cContext,ok = this.channels[channel] ; !ok {
			cContext = &channelContext{minheap:util.NewMinHeap(1024),pendingCalls:map[uint64]*reqContext{}}
			this.channels[channel] = cContext
		}
		cContext.pendingCalls[context.seq] = context
		cContext.minheap.Insert(context)
	}
}

func (this *reqContextMgr) onClose(channel RPCChannel,err error) {
	if cContext,ok := this.channels[channel] ; ok {
		delete(this.channels,channel)
		for _ , v := range cContext.pendingCalls {
			v.callResponseCB(nil,err)
		}
	}	
}

func (this *reqContextMgr) onResponse(channel RPCChannel,rpcMsg RPCMessage) {
	var ok bool
	var cContext *channelContext
	var context  *reqContext
	if cContext,ok = this.channels[channel] ; ok {
		if context,ok = cContext.pendingCalls[rpcMsg.GetSeq()] ; ok {
			//kendynet.Debugf("on response reqContext:%d\n",rpcMsg.GetSeq())
			delete(cContext.pendingCalls,rpcMsg.GetSeq())
			cContext.minheap.Remove(context)
			resp := rpcMsg.(*RPCResponse)
			context.callResponseCB(resp.Ret,resp.Err)			
		} else {			
			kendynet.Debugf("on response,but missing reqContext:%d\n",rpcMsg.GetSeq())
			fmt.Fprintf(os.Stderr,"on response,but missing reqContext:%d\n",rpcMsg.GetSeq())
		}	
	} else {
		kendynet.Debugf("on response,but missing channel reqContext:%d\n",rpcMsg.GetSeq())		
	} 
}

func (this *reqContextMgr) checkTimeout() {
	now := time.Now()
	for _,v := range this.channels {
		for {
			r := v.minheap.Min()
			if r != nil && now.After(r.(*reqContext).deadline) {
				v.minheap.PopMin()
				if _,ok := v.pendingCalls[r.(*reqContext).seq];!ok {
					kendynet.Infof("timeout context:%d not found\n",r.(*reqContext).seq)
				}else{
					delete(v.pendingCalls,r.(*reqContext).seq)
					kendynet.Infof("timeout context:%d\n",r.(*reqContext).seq)
					fmt.Fprintf(os.Stderr,"timeout context:%d\n",r.(*reqContext).seq)				
					r.(*reqContext).callResponseCB(nil,ErrCallTimeout)
				}
			} else {
				break
			}
		}
	}	
}


var reqMgr *reqContextMgr

type RPCClient struct {
	encoder   		  	RPCMessageEncoder
	decoder   		  	RPCMessageDecoder
	sequence            uint64
	channel             RPCChannel         
}

//通道关闭后调用
func (this *RPCClient) OnChannelClose(err error) {
	reqMgr.queue.Post(func(){
		reqMgr.onClose(this.channel,err)
	})
}

//收到RPC消息后调用
func (this *RPCClient) OnRPCMessage(message interface{}) {
	msg,err := this.decoder.Decode(message)
	if nil != err {
		Errorf(util.FormatFileLine("RPCClient rpc message from(%s) decode err:%s\n",this.channel.Name,err.Error()))
		return
	}	
	reqMgr.queue.Post(func(){
		reqMgr.onResponse(this.channel,msg)
	})
}

//投递，不关心响应和是否失败
func (this *RPCClient) Post(method string,arg interface{}) error {

	req := &RPCRequest{
		Method : method,
		Seq : atomic.AddUint64(&this.sequence,1), 
		Arg : arg,
		NeedResp : false,
	}

	request,err := this.encoder.Encode(req)
	if err != nil {
		return fmt.Errorf("encode error:%s\n",err.Error())
	} 

	err = this.channel.SendRequest(request)
	if nil != err {
		return err
	}
	return nil
}


func AsynHandler(cb RPCResponseHandler) RPCResponseHandler {
	if nil != cb {
		return func (r interface{},e error) {
			go cb(r,e)
		}
	} else {
		return nil
	}
}

/*
*  异步调用
*  cb将在一个单独的go程中执行,如需在cb中调用阻塞函数请使用AsynHandler封装cb
*/
func (this *RPCClient) AsynCall(method string,arg interface{},timeout uint32,cb RPCResponseHandler) error {

	if cb == nil {
		return fmt.Errorf("cb == nil")
	}

	reqMgr.queue.Post(func (){
		req := &RPCRequest{ 
			Method : method,
			Seq : atomic.AddUint64(&this.sequence,1), 
			Arg : arg,
			NeedResp : true,
		}

		request,err := this.encoder.Encode(req)
		if err != nil {
			cb(nil,fmt.Errorf("encode error:%s\n",err.Error()))
			return
		} 

		if timeout <= 0 {
			timeout = 5000
		}

		context := &reqContext{}
		context.heapIdx = 0
		context.seq = req.Seq
		context.onResponse = cb
		context.deadline = time.Now().Add(time.Duration(timeout) * time.Millisecond)
		context.timestamp = time.Now().UnixNano()
		reqMgr.onRequest(this.channel,context,request)
	})
	return nil
}

//同步调用
//同步调用
func (this *RPCClient) SyncCall(method string,arg interface{},timeout uint32) (ret interface{},err error) {
	respChan := make(chan interface{})
	f := func (ret_ interface{},err_ error) {
		ret = ret_
		err = err_
		respChan <- nil	
	}
	if err = this.AsynCall(method,arg,timeout,f); err != nil {
		return
	}
	_ = <- respChan
	return
}

func NewClient(channel RPCChannel,decoder RPCMessageDecoder,encoder RPCMessageEncoder) (*RPCClient,error) {
	if nil == decoder {
		return nil,fmt.Errorf("decoder == nil")
	}

	if nil == encoder {
		return nil,fmt.Errorf("encoder == nil")
	}

	if nil == channel {
		return nil,fmt.Errorf("channel == nil")
	}

	return &RPCClient{encoder:encoder,decoder:decoder,channel:channel},nil
}

var client_once sync.Once

func InitClient(queue *event.EventQueue) {

	client_once.Do(func(){
		reqMgr = &reqContextMgr{
			channels:map[RPCChannel]*channelContext{},
		}

		if nil != queue {
			reqMgr.queue = queue
		} else {
			reqMgr.queue = event.NewEventQueue()
			go func(){
				reqMgr.queue.Run()
			}()
		}

		//启动一个go程，每10毫秒向queue投递一个定时器消息
		go func() {
			for {
				time.Sleep(time.Duration(10)*time.Millisecond)
				reqMgr.queue.Post(func (){
					reqMgr.checkTimeout()				
				})
			}
		}()
	})
}