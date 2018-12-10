package client

import (
	"flyfish/codec"
	"flyfish/errcode"
	"github.com/sniperHW/kendynet"
	"github.com/sniperHW/kendynet/util"
	"github.com/sniperHW/kendynet/event"
	"github.com/sniperHW/kendynet/socket/stream_socket/tcp"
	protocol "flyfish/proto"
	"github.com/golang/protobuf/proto"
	"time"
	"runtime"
	"fmt"
	"sync/atomic"
)

const requestTimeout = 5*time.Second
const pingTime       = 5

type Client struct {
	session        kendynet.StreamSession
	seqno          int64
	addr           string
	minheap        *util.MinHeap           //超时小根堆
	pendingSend    []*cmdContext           //等待发送的消息
	waitResp       map[int64]*cmdContext   //等待响应的消息
	eventQueue     *event.EventQueue       //此客户端的主处理队列
	callbackQueue  *event.EventQueue       //响应回调的事件队列
	dialing        bool
	closed         int32
	nextPing       int64
}

func OpenClient(addr string,callbackQueue ...*event.EventQueue) *Client {
	c := &Client{
		addr          : addr,
		eventQueue    : event.NewEventQueue(),
		waitResp      : map[int64]*cmdContext{},
		minheap       : util.NewMinHeap(65535),
		pendingSend   : []*cmdContext{},
	}

	if len(callbackQueue) > 0 {
		c.callbackQueue = callbackQueue[0]
	}

	c.startEventQueue()
	c.dial()
	c.startTimeoutChecker()
	return c
}

func (this *Client) onClose() {
	if nil != this.session {
		this.session.Close("",0)
		this.minheap.Clear()
		this.eventQueue.Close()

		for _,c := range(this.pendingSend) {
			if c.status != wait_remove {
				c.result.ErrCode = errcode.ERR_CLOSE
				this.doCallBack(c)			
			}
		}

		for _,c := range(this.waitResp) {
			c.result.ErrCode = errcode.ERR_CLOSE
			this.doCallBack(c)
		}				
	}
}

func (this *Client) Close() {
	if atomic.CompareAndSwapInt32(&this.closed,0,1) {
		this.eventQueue.Post(func(){
			this.onClose()
		})
	}
}

func (this *Client) startEventQueue() {
	go func(){
		this.eventQueue.Run()
	}()	
}

func (this *Client) checkTimeout(now *time.Time) {
	for {
		cc := this.minheap.Min()
		if cc != nil && now.After(cc.(*cmdContext).deadline) {
			this.minheap.PopMin()
			c := cc.(*cmdContext)
			c.result.ErrCode = errcode.ERR_TIMEOUT
			if c.status == wait_send {
				c.status = wait_remove
				this.doCallBack(c)
			} else {
				if _,ok := this.waitResp[c.seqno];!ok{
					kendynet.Infof("timeout cmdContext:%d not found\n",c.seqno)					
				} else {
					kendynet.Infof("timeout cmdContext:%d\n",c.seqno)
					delete(this.waitResp,c.seqno)
					this.doCallBack(c)
				}
			}
		} else {
			break
		}
	}	
}

func (this *Client) ping(now *time.Time) {
	if nil != this.session && now.Unix() > this.nextPing {
		req := &protocol.PingReq{
			Timestamp : proto.Int64(now.UnixNano()),
		}
		this.session.Send(req)		
	}
}

func (this *Client) startTimeoutChecker() {
	go func(){
		for atomic.LoadInt32(&this.closed) == 0 {
			time.Sleep(time.Duration(10)*time.Millisecond)
			this.eventQueue.Post(func (){
				now := time.Now()
				//this.ping(&now)
				this.checkTimeout(&now)				
			})
		}
	}()	
}

func (this *Client) removeContext(seqno int64) *cmdContext {
	c := this.waitResp[seqno]
	if nil == c {
		return nil
	} else {
		this.minheap.Remove(c)
		delete(this.waitResp,seqno)
		return c		
	}
}

func pcall(c *cmdContext) {
	defer func(){
		if r := recover(); r != nil {
			buf := make([]byte, 65535)
			l := runtime.Stack(buf, false)
			kendynet.Errorf("%v: %s\n", r, buf[:l])
		}			
	}()
	c.callback(&c.result)		
}

func (this *Client) doCallBack(c *cmdContext) {
	if nil != this.callbackQueue {
		this.callbackQueue.Post(func(){
			c.callback(&c.result)
		})
	} else {
		pcall(c)
	}	
}

func (this *Client) onConnected(session kendynet.StreamSession) {
	fmt.Println("Client onConnected")
	this.eventQueue.Post(func(){
		this.dialing  = false
		this.session  = session
		this.nextPing = time.Now().Unix() + pingTime 
		//session.SetRecvTimeout(common.HeartBeat_Timeout * time.Second)
		this.session.SetReceiver(codec.NewReceiver())
		this.session.SetEncoder(codec.NewEncoder())
		this.session.SetCloseCallBack(func(sess kendynet.StreamSession, reason string) {
			if atomic.LoadInt32(&this.closed) == 0 {
				this.onDisconnected()
			}
		})
		this.session.Start(func(event *kendynet.Event) {
			if event.EventType == kendynet.EventTypeError {
				event.Session.Close(event.Data.(error).Error(), 0)
			} else {
				this.onMessage(event.Data.(*codec.Message))
			}
		})

		//发送被排队的请求
		fmt.Println("send pending req",len(this.pendingSend))
		for _,v := range(this.pendingSend) {
			//fmt.Println(v.status)
			if v.status != wait_remove {
				this.sendReq(v)
			}
		}
	})
}

func (this *Client) onDisconnected() {
	this.eventQueue.Post(func(){
		this.session = nil
		this.minheap.Clear()
		for _,c := range(this.waitResp) {
			c.result.ErrCode = errcode.ERR_DISCONNECTED
			this.doCallBack(c)
		}
		this.dial()
	})	
}

func (this *Client) dial() {

	if this.dialing {
		return
	}

	this.dialing = true

	fmt.Println("Client dial")

	go func() {
		connector, _ := tcp.NewConnector("tcp", this.addr)
		for {
			session,err := connector.Dial(time.Second*5)
			if nil == err {
				this.onConnected(session)
				return
			}
		}	
	}()
}


func (this *Client) sendReq(c *cmdContext) {
	err := this.session.Send(c.req)

	if nil == err {
		c.status = wait_resp
		this.waitResp[c.seqno] = c
	} else {
		//记录日志
		this.minheap.Remove(c)
		c.result.ErrCode = errcode.ERR_SEND
		this.doCallBack(c)
	}
}

func (this *Client) exec(c *cmdContext) {

	//fmt.Println("Client exec")

	if atomic.LoadInt32(&this.closed) == 1 {
		c.result.ErrCode = errcode.ERR_CLOSE
		this.doCallBack(c)
		return
	}

	this.eventQueue.Post(func(){
		c.deadline = time.Now().Add(requestTimeout)
		this.minheap.Insert(c)
		if nil == this.session || this.dialing {
			c.status = wait_send
			this.pendingSend = append(this.pendingSend,c)
		} else {
			this.sendReq(c)
		}
	})
}