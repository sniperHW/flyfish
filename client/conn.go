package client

import (
	"flyfish/codec"
	"flyfish/errcode"
	protocol "flyfish/proto"
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/sniperHW/kendynet"
	"github.com/sniperHW/kendynet/event"
	connector "github.com/sniperHW/kendynet/socket/connector/tcp"
	"github.com/sniperHW/kendynet/util"
	"sync/atomic"
	"time"
)

const requestTimeout = 5 * time.Second
const pingTime = 5

type Conn struct {
	session     kendynet.StreamSession
	seqno       int64
	addr        string
	minheap     *util.MinHeap         //超时小根堆
	pendingSend []*cmdContext         //等待发送的消息
	waitResp    map[int64]*cmdContext //等待响应的消息
	eventQueue  *event.EventQueue     //此客户端的主处理队列
	dialing     bool
	closed      int32
	nextPing    int64
	c           *Client
}

func openConn(cli *Client, addr string) *Conn {
	c := &Conn{
		addr:        addr,
		eventQueue:  event.NewEventQueue(),
		waitResp:    map[int64]*cmdContext{},
		minheap:     util.NewMinHeap(1024),
		pendingSend: []*cmdContext{},
		c:           cli,
	}

	c.startEventQueue()
	c.dial()
	c.startTimeoutChecker()
	return c
}

func (this *Conn) onClose() {
	if nil != this.session {
		this.session.Close("", 0)
		this.minheap.Clear()
		this.eventQueue.Close()

		for _, c := range this.pendingSend {
			if c.status != wait_remove {
				this.c.doCallBack(c.cb, errcode.ERR_CLOSE)
			}
		}
		for _, c := range this.waitResp {
			this.c.doCallBack(c.cb, errcode.ERR_CLOSE)
		}
	}
}

func (this *Conn) Close() {
	if atomic.CompareAndSwapInt32(&this.closed, 0, 1) {
		this.eventQueue.Post(func() {
			this.onClose()
		})
	}
}

func (this *Conn) startEventQueue() {
	go func() {
		this.eventQueue.Run()
	}()
}

func (this *Conn) checkTimeout(now *time.Time) {
	for {
		cc := this.minheap.Min()
		if cc != nil && now.After(cc.(*cmdContext).deadline) {
			this.minheap.PopMin()
			c := cc.(*cmdContext)
			if c.status == wait_send {
				c.status = wait_remove
				this.c.doCallBack(c.cb, errcode.ERR_TIMEOUT)
			} else {
				if _, ok := this.waitResp[c.seqno]; !ok {
					kendynet.Infof("timeout cmdContext:%d not found\n", c.seqno)
				} else {
					kendynet.Infof("timeout cmdContext:%d\n", c.seqno)
					delete(this.waitResp, c.seqno)
					this.c.doCallBack(c.cb, errcode.ERR_TIMEOUT)
				}
			}
		} else {
			break
		}
	}
}

func (this *Conn) ping(now *time.Time) {
	if nil != this.session && now.Unix() > this.nextPing {
		req := &protocol.PingReq{
			Timestamp: proto.Int64(now.UnixNano()),
		}
		this.session.Send(req)
	}
}

func (this *Conn) startTimeoutChecker() {
	go func() {
		for atomic.LoadInt32(&this.closed) == 0 {
			time.Sleep(time.Duration(10) * time.Millisecond)
			this.eventQueue.Post(func() {
				now := time.Now()
				//this.ping(&now)
				this.checkTimeout(&now)
			})
		}
	}()
}

func (this *Conn) removeContext(seqno int64) *cmdContext {
	c := this.waitResp[seqno]
	if nil == c {
		return nil
	} else {
		this.minheap.Remove(c)
		delete(this.waitResp, seqno)
		return c
	}
}

func (this *Conn) onConnected(session kendynet.StreamSession) {
	this.eventQueue.Post(func() {
		this.dialing = false
		this.session = session
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
		fmt.Println("send pending req", len(this.pendingSend))
		for _, v := range this.pendingSend {
			//fmt.Println(v.status)
			if v.status != wait_remove {
				this.sendReq(v)
			}
		}
		this.pendingSend = []*cmdContext{}
	})
}

func (this *Conn) onDisconnected() {
	this.eventQueue.Post(func() {
		this.session = nil
		this.minheap.Clear()

		for _, c := range this.waitResp {
			this.c.doCallBack(c.cb, errcode.ERR_DISCONNECTED)
		}

		this.dial()
	})
}

func (this *Conn) dial() {

	if this.dialing {
		return
	}

	this.dialing = true

	fmt.Println("Conn dial", this.addr)

	go func() {
		c, _ := connector.New("tcp", this.addr)
		for {
			session, err := c.Dial(time.Second * 5)
			if nil == err {
				this.onConnected(session)
				return
			}
		}
	}()
}

func (this *Conn) sendReq(c *cmdContext) {
	err := this.session.Send(c.req)
	if nil == err {
		c.status = wait_resp
		this.waitResp[c.seqno] = c
	} else {
		//记录日志
		this.minheap.Remove(c)
		if err == kendynet.ErrSendQueFull {
			this.c.doCallBack(c.cb, errcode.ERR_BUSY)
		} else {
			this.c.doCallBack(c.cb, errcode.ERR_SEND)
		}
	}
}

func (this *Conn) exec(c *cmdContext) {
	this.eventQueue.Post(func() {
		if atomic.LoadInt32(&this.closed) == 1 {
			this.c.doCallBack(c.cb, errcode.ERR_CLOSE)
		} else {
			c.deadline = time.Now().Add(requestTimeout)
			this.minheap.Insert(c)
			if nil == this.session || this.dialing {
				c.status = wait_send
				this.pendingSend = append(this.pendingSend, c)
			} else {
				this.sendReq(c)
			}
		}
	})
}
