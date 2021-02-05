package client

import (
	"github.com/sniperHW/flyfish/errcode"
	"github.com/sniperHW/flyfish/net"
	"github.com/sniperHW/flyfish/net/pb"
	"github.com/sniperHW/kendynet"
	//"github.com/sniperHW/kendynet/event"
	"github.com/sniperHW/kendynet/timer"
	"sync"
	"time"
)

const maxPendingSize int = 10000

type Conn struct {
	sync.Mutex
	session     kendynet.StreamSession
	addr        string
	pendingSend []*cmdContext //等待发送的消息
	waitResp    map[uint64]*cmdContext
	dialing     bool
	c           *Client
	timerMgr    *timer.TimerMgr
}

func openConn(cli *Client, addr string) *Conn {
	c := &Conn{
		addr:        addr,
		pendingSend: []*cmdContext{},
		waitResp:    map[uint64]*cmdContext{},
		c:           cli,
		timerMgr:    timer.NewTimerMgr(1),
	}
	return c
}

/*
func (this *Conn) ping(now *time.Time) {
	if nil != this.session && now.After(this.nextPing) {
		this.nextPing = now.Add(protocol.PingTime)

		req := net.NewMessage(net.CommonHead{}, &protocol.PingReq{
			Timestamp: now.UnixNano(),
		})

		this.session.Send(req)
	}
}*/

func (this *Conn) onConnected(session kendynet.StreamSession, compress bool) {

	this.Lock()

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

	this.Unlock()

	now := time.Now()

	//发送被排队的请求
	for _, v := range pendingSend {
		//已经超时或马上就要超时的请求不发送
		if !v.isTimeouted && v.deadline.Sub(now) > 10*time.Millisecond {
			this.sendReq(v)
		}
	}
}

func (this *Conn) onTimeout(_ *timer.Timer, ctx interface{}) {
	this.Lock()
	c := ctx.(*cmdContext)
	_, ok := this.waitResp[uint64(c.req.GetHead().Seqno)]
	if ok {
		delete(this.waitResp, uint64(c.req.GetHead().Seqno))
	}
	this.Unlock()

	if ok {
		c.isTimeouted = true
		this.c.doCallBack(c.unikey, c.cb, errcode.ERR_TIMEOUT)
	}
}

func (this *Conn) onDisconnected() {
	this.Lock()
	this.session = nil
	waitResp := this.waitResp
	this.waitResp = map[uint64]*cmdContext{}
	this.Unlock()

	for k, v := range waitResp {
		this.timerMgr.CancelByIndex(k)
		this.c.doCallBack(v.unikey, v.cb, errcode.ERR_CONNECTION)
	}
}

func (this *Conn) dial() {
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

func (this *Conn) exec(c *cmdContext) {
	errCode := errcode.ERR_OK
	this.Lock()
	defer func() {
		this.Unlock()
		if errCode != errcode.ERR_OK {
			this.c.doCallBack(c.unikey, c.cb, errCode)
		}
	}()
	c.deadline = time.Now().Add(time.Duration(ClientTimeout) * time.Millisecond)
	if nil == this.session && !this.dialing {
		this.dial()
	}

	if this.dialing {
		if len(this.pendingSend) < maxPendingSize {
			this.waitResp[uint64(c.req.GetHead().Seqno)] = c
			this.timerMgr.OnceWithIndex(time.Duration(ClientTimeout)*time.Millisecond, func(t *timer.Timer, ctx interface{}) {
				this.onTimeout(t, ctx)
			}, c, uint64(c.req.GetHead().Seqno))
			this.pendingSend = append(this.pendingSend, c)
		} else {
			errCode = errcode.ERR_BUSY
		}
	} else {
		this.waitResp[uint64(c.req.GetHead().Seqno)] = c
		this.timerMgr.OnceWithIndex(time.Duration(ClientTimeout)*time.Millisecond, func(t *timer.Timer, ctx interface{}) {
			this.onTimeout(t, ctx)
		}, c, uint64(c.req.GetHead().Seqno))
		this.sendReq(c)
	}
}
