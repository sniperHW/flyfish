package client

import (
	"github.com/sniperHW/flyfish/errcode"
	"github.com/sniperHW/flyfish/pkg/net"
	"github.com/sniperHW/flyfish/pkg/net/cs"
	"sync"
	"time"
)

var maxPendingSize int = 10000

type Conn struct {
	sync.Mutex
	session     *net.Socket
	addr        string
	pendingSend []*cmdContext //等待发送的消息
	waitResp    map[int64]*cmdContext
	dialing     bool
	c           *Client
}

func openConn(cli *Client, addr string) *Conn {
	c := &Conn{
		addr:        addr,
		pendingSend: []*cmdContext{},
		waitResp:    map[int64]*cmdContext{},
		c:           cli,
	}
	return c
}

func (this *Conn) onConnected(session *net.Socket) {

	this.Lock()
	defer this.Unlock()

	this.dialing = false
	this.session = session
	this.session.SetSendQueueSize(maxPendingSize)
	this.session.SetInBoundProcessor(cs.NewRespInboundProcessor())
	this.session.SetEncoder(&cs.ReqEncoder{})
	this.session.SetCloseCallBack(func(sess *net.Socket, reason error) {
		GetSugar().Infof("socket close %v", reason)
		this.onDisconnected()
	}).BeginRecv(func(s *net.Socket, msg interface{}) {
		this.onMessage(msg.(*cs.RespMessage))
	})

	pendingSend := this.pendingSend
	this.pendingSend = []*cmdContext{}

	now := time.Now()

	//发送被排队的请求
	for _, v := range pendingSend {
		//已经超时或马上就要超时的请求不发送
		if v.deadline.Sub(now) > 10*time.Millisecond && nil != this.waitResp[v.req.Seqno] {
			this.sendReq(v)
		}
	}
}

func (this *Conn) onDisconnected() {
	this.Lock()
	this.session = nil
	waitResp := this.waitResp
	this.waitResp = map[int64]*cmdContext{}
	this.Unlock()

	for _, v := range waitResp {
		if v.deadlineTimer.Stop() {
			this.c.doCallBack(v.unikey, v.cb, errcode.New(errcode.Errcode_error, "lose connection"))
			releaseCmdContext(v)
		}
	}
}

func (this *Conn) dial() {
	this.dialing = true

	go func() {
		c := cs.NewConnector("tcp", this.addr)
		for {
			session, err := c.Dial(time.Second * 5)
			if nil == err {
				this.onConnected(session)
				return
			} else {
				GetSugar().Errorf("dial %s error:%s", this.addr, err.Error())
				time.Sleep(1 * time.Second)
			}
		}
	}()
}

func (this *Conn) sendReq(c *cmdContext) {
	if nil != this.c.unikeyPlacement {
		//如果提供了定位器，使用定位器直接计算出Store
		c.req.Store = this.c.unikeyPlacement(c.req.UniKey)
	}
	this.session.Send(c.req)
}

func (this *Conn) exec(c *cmdContext) {
	var errCode errcode.Error
	this.Lock()
	defer func() {
		this.Unlock()
		if errCode != nil {
			this.c.doCallBack(c.unikey, c.cb, errCode)
			releaseCmdContext(c)
		}
	}()
	c.deadline = time.Now().Add(time.Duration(ClientTimeout) * time.Millisecond)
	if nil == this.session && !this.dialing {
		this.dial()
	}

	if this.dialing {
		if len(this.pendingSend) < maxPendingSize {
			this.waitResp[c.req.Seqno] = c
			c.deadlineTimer = time.AfterFunc(time.Duration(ClientTimeout)*time.Millisecond, c.onTimeout)
			this.pendingSend = append(this.pendingSend, c)
		} else {
			errCode = errcode.New(errcode.Errcode_retry, "busy please retry later")
		}
	} else {
		this.waitResp[c.req.Seqno] = c
		c.deadlineTimer = time.AfterFunc(time.Duration(ClientTimeout)*time.Millisecond, c.onTimeout)
		this.sendReq(c)
	}
}
