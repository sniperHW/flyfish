package client

import (
	//"github.com/sniperHW/flyfish/codec"
	"github.com/sniperHW/flyfish/errcode"
	"github.com/sniperHW/flyfish/net/pb"
	protocol "github.com/sniperHW/flyfish/proto"
	//"github.com/sniperHW/flyfish/proto/login"
	"github.com/sniperHW/kendynet"
	"github.com/sniperHW/kendynet/event"
	//connector "github.com/sniperHW/kendynet/socket/connector/tcp"
	"github.com/sniperHW/flyfish/net"
	"github.com/sniperHW/kendynet/timer"
	"time"
)

const maxPendingSize int = 10000

type Conn struct {
	session     kendynet.StreamSession
	addr        string
	pendingSend []*cmdContext     //等待发送的消息
	eventQueue  *event.EventQueue //此客户端的主处理队列
	dialing     bool
	c           *Client
	nextPing    time.Time
	timerMgr    *timer.TimerMgr
}

func openConn(cli *Client, addr string) *Conn {
	c := &Conn{
		addr:        addr,
		eventQueue:  event.NewEventQueue(),
		pendingSend: []*cmdContext{},
		c:           cli,
		nextPing:    time.Now().Add(protocol.PingTime),
		timerMgr:    timer.NewTimerMgr(1),
	}
	go c.eventQueue.Run()
	return c
}

func (this *Conn) ping(now *time.Time) {
	if nil != this.session && now.After(this.nextPing) {
		this.nextPing = now.Add(protocol.PingTime)

		req := net.NewMessage(net.CommonHead{}, &protocol.PingReq{
			Timestamp: now.UnixNano(),
		})

		this.session.Send(req)
	}
}

func (this *Conn) onConnected(session kendynet.StreamSession, compress bool) {
	this.eventQueue.Post(this.c.priority, func() {
		this.dialing = false
		this.session = session
		this.session.SetSendQueueSize(maxPendingSize)
		this.session.SetReceiver(net.NewReceiver(pb.GetNamespace("response"), compress))
		this.session.SetEncoder(net.NewEncoder(pb.GetNamespace("request"), compress))
		this.session.SetCloseCallBack(func(sess kendynet.StreamSession, reason string) {
			this.onDisconnected()
		})
		this.session.Start(func(event *kendynet.Event) {
			if event.EventType == kendynet.EventTypeError {
				event.Session.Close(event.Data.(error).Error(), 0)
			} else {
				this.onMessage(event.Data.(*net.Message))
			}
		})

		pendingSend := this.pendingSend
		this.pendingSend = []*cmdContext{}

		now := time.Now()

		//发送被排队的请求
		for _, v := range pendingSend {
			//已经超时或马上就要超时的请求不发送
			if !v.isTimeouted && v.deadline.Sub(now) > 10*time.Millisecond {
				this.sendReq(v)
			}
		}

	})
}

func (this *Conn) onDisconnected() {
	this.eventQueue.Post(this.c.priority, func() {
		this.session = nil
	})
}

func (this *Conn) dial() {
	if this.dialing {
		return
	}

	this.dialing = true

	go func() {
		c := net.NewConnector("tcp", this.addr, this.c.compress)
		for {
			session, compress, err := c.Dial(time.Second * 5)
			if nil == err {
				this.onConnected(session, compress)
				return
			} else {
				logger.Errorln("dial error", this.addr, err)
				time.Sleep(1 * time.Second)
			}
		}
	}()
}

func (this *Conn) sendReq(c *cmdContext) {
	this.session.Send(c.req)
}

func (this *Conn) onTimeout(_ *timer.Timer, ctx interface{}) {
	c := ctx.(*cmdContext)
	c.isTimeouted = true
	this.c.doCallBack(c.unikey, c.cb, errcode.ERR_TIMEOUT)
}

func (this *Conn) exec(c *cmdContext) {
	this.eventQueue.Post(this.c.priority, func() {
		c.deadline = time.Now().Add(time.Duration(ClientTimeout) * time.Millisecond)
		if nil == this.session && !this.dialing {
			this.dial()
		}

		if this.dialing {
			if len(this.pendingSend) < maxPendingSize {
				this.timerMgr.OnceWithIndex(time.Duration(ClientTimeout)*time.Millisecond, func(t *timer.Timer, ctx interface{}) {
					this.eventQueue.PostNoWait(this.c.priority, this.onTimeout, t, ctx)
				}, c, uint64(c.req.GetHead().Seqno))
				this.pendingSend = append(this.pendingSend, c)
			} else {
				this.c.doCallBack(c.unikey, c.cb, errcode.ERR_BUSY)
			}
		} else {
			this.timerMgr.OnceWithIndex(time.Duration(ClientTimeout)*time.Millisecond, func(t *timer.Timer, ctx interface{}) {
				this.eventQueue.PostNoWait(this.c.priority, this.onTimeout, t, ctx)
			}, c, uint64(c.req.GetHead().Seqno))
			this.sendReq(c)
		}
	})
}
