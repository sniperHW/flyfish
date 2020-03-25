package client

import (
	"github.com/sniperHW/flyfish/codec"
	"github.com/sniperHW/flyfish/codec/pb"
	"github.com/sniperHW/flyfish/errcode"
	protocol "github.com/sniperHW/flyfish/proto"
	"github.com/sniperHW/flyfish/proto/login"
	"github.com/sniperHW/kendynet"
	"github.com/sniperHW/kendynet/event"
	connector "github.com/sniperHW/kendynet/socket/connector/tcp"
	"github.com/sniperHW/kendynet/timer"
	"github.com/sniperHW/kendynet/util"
	//"net"
	"time"
)

const maxPendingSize int = 10000

type Conn struct {
	session     kendynet.StreamSession
	seqno       int64
	addr        string
	minheap     util.MinHeap          //超时小根堆
	pendingSend []*cmdContext         //等待发送的消息
	waitResp    map[int64]*cmdContext //等待响应的消息
	eventQueue  *event.EventQueue     //此客户端的主处理队列
	dialing     bool
	c           *Client
	nextPing    time.Time
	timer       *timer.Timer
}

func openConn(cli *Client, addr string) *Conn {
	c := &Conn{
		addr:        addr,
		eventQueue:  event.NewEventQueue(),
		waitResp:    map[int64]*cmdContext{},
		minheap:     util.NewMinHeap(1024),
		pendingSend: []*cmdContext{},
		c:           cli,
		nextPing:    time.Now().Add(protocol.PingTime),
	}
	go c.eventQueue.Run()
	c.timer = timer.Repeat(time.Duration(100)*time.Millisecond, c.eventQueue, func(_ *timer.Timer, _ interface{}) {
		now := time.Now()
		//不主动发心跳，如果空闲就让服务器释放连接
		//c.ping(&now)
		c.checkTimeout(&now)
	}, nil)
	return c
}

func (this *Conn) checkTimeout(now *time.Time) {
	for {
		cc := this.minheap.Min()
		if cc != nil && now.After(cc.(*cmdContext).deadline) {
			this.minheap.PopMin()
			c := cc.(*cmdContext)
			if c.status == wait_send {
				c.status = wait_remove
				this.c.doCallBack(c.unikey, c.cb, errcode.ERR_TIMEOUT)
			} else {

				seqno := c.req.GetHead().Seqno

				if _, ok := this.waitResp[seqno]; ok {
					delete(this.waitResp, seqno)
					this.c.doCallBack(c.unikey, c.cb, errcode.ERR_TIMEOUT)
				}
			}
		} else {
			break
		}
	}
}

func (this *Conn) ping(now *time.Time) {
	if nil != this.session && now.After(this.nextPing) {
		this.nextPing = now.Add(protocol.PingTime)

		req := codec.NewMessage(codec.CommonHead{}, &protocol.PingReq{
			Timestamp: now.UnixNano(),
		})

		this.session.Send(req)
	}
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

	if !login.SendLoginReq(session, &protocol.LoginReq{Compress: this.c.compress}) {
		session.Close("login failed", 0)
		this.eventQueue.Post(func() {
			this.dialing = false
			this.dial()
		})
		return
	}

	loginResp, err := login.RecvLoginResp(session)
	if nil != err || !loginResp.GetOk() {
		session.Close("login failed", 0)
		this.eventQueue.Post(func() {
			this.dialing = false
			this.dial()
		})
		return
	}

	this.eventQueue.Post(func() {
		this.dialing = false
		this.session = session
		this.session.SetSendQueueSize(maxPendingSize)
		this.session.SetReceiver(codec.NewReceiver(pb.GetNamespace("response"), loginResp.GetCompress()))
		this.session.SetEncoder(codec.NewEncoder(pb.GetNamespace("request"), loginResp.GetCompress()))
		this.session.SetCloseCallBack(func(sess kendynet.StreamSession, reason string) {
			this.onDisconnected()
		})
		this.session.Start(func(event *kendynet.Event) {
			if event.EventType == kendynet.EventTypeError {
				event.Session.Close(event.Data.(error).Error(), 0)
			} else {
				this.onMessage(event.Data.(*codec.Message))
			}
		})

		pendingSend := this.pendingSend
		this.pendingSend = []*cmdContext{}

		//发送被排队的请求
		for _, v := range pendingSend {
			if v.status != wait_remove {
				this.sendReq(v)
			}
		}

	})
}

func (this *Conn) onDisconnected() {
	this.eventQueue.Post(func() {
		this.session = nil
		this.minheap.Clear()
		waitResp := this.waitResp
		this.waitResp = map[int64]*cmdContext{}
		for _, c := range waitResp {
			this.c.doCallBack(c.unikey, c.cb, errcode.ERR_CONNECTION)
		}
	})
}

func (this *Conn) dial() {
	if this.dialing {
		return
	}

	this.dialing = true

	go func() {
		c, _ := connector.New("tcp", this.addr)
		for {
			session, err := c.Dial(time.Second * 5)
			if nil == err {
				this.onConnected(session)
				return
			} else {
				logger.Errorln("dial error", this.addr, err)
				time.Sleep(1 * time.Second)
			}
		}
	}()
}

func (this *Conn) sendReq(c *cmdContext) {
	err := this.session.Send(c.req)
	if nil == err {
		c.status = wait_resp
		this.waitResp[c.req.GetHead().Seqno] = c
	} else {
		//记录日志
		this.minheap.Remove(c)
		if err == kendynet.ErrSendQueFull {
			this.c.doCallBack(c.unikey, c.cb, errcode.ERR_BUSY)
		} else {
			this.c.doCallBack(c.unikey, c.cb, errcode.ERR_CONNECTION)
		}
	}
}

func (this *Conn) exec(c *cmdContext) {
	this.eventQueue.Post(func() {
		c.deadline = time.Now().Add(time.Duration(ClientTimeout) * time.Millisecond)
		if nil == this.session && !this.dialing {
			this.dial()
		}

		if this.dialing {
			if len(this.pendingSend) < maxPendingSize {
				this.minheap.Insert(c)
				c.status = wait_send
				this.pendingSend = append(this.pendingSend, c)
			} else {
				this.c.doCallBack(c.unikey, c.cb, errcode.ERR_BUSY)
			}
		} else {
			this.minheap.Insert(c)
			this.sendReq(c)
		}
	})
}
