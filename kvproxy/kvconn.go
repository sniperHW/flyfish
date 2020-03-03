package kvproxy

import (
	protocol "github.com/sniperHW/flyfish/proto"
	"github.com/sniperHW/flyfish/proto/login"
	"github.com/sniperHW/kendynet"
	connector "github.com/sniperHW/kendynet/socket/connector/tcp"
	"sync"
	"time"
)

const maxSendQueueSize = 10000

//存放连接建立前待发送的消息
type pendingMsg struct {
	sendDeadline time.Time
	msg          *kendynet.ByteBuffer
}

type Conn struct {
	sync.Mutex
	session     kendynet.StreamSession
	serverID    int
	dialing     bool
	addr        string
	pendingSend []*pendingMsg
	compress    bool
	proxy       *kvproxy
}

func openConn(proxy *kvproxy, serverID int, addr string, compress bool) *Conn {
	c := &Conn{
		addr:        addr,
		serverID:    serverID,
		pendingSend: []*pendingMsg{},
		compress:    compress,
		proxy:       proxy,
	}
	return c
}

func (this *Conn) Close() {
	this.Lock()
	this.pendingSend = []*pendingMsg{}
	if nil != this.session {
		this.Unlock()
		this.session.Close("", 0)
	} else {
		this.Unlock()
	}
}

func (this *Conn) onConnected(session kendynet.StreamSession) {

	defer func() {
		this.Lock()
		this.dialing = false
		this.Unlock()
	}()

	if !login.SendLoginReq(session, &protocol.LoginReq{Compress: this.compress}) {
		session.Close("login failed", 0)
		return
	}

	loginResp, err := login.RecvLoginResp(session)
	if nil != err || !loginResp.GetOk() {
		session.Close("login failed", 0)
		return
	}

	this.Lock()
	this.session = session
	this.session.SetSendQueueSize(maxSendQueueSize)
	this.session.SetReceiver(NewReceiver())

	this.session.SetCloseCallBack(func(sess kendynet.StreamSession, reason string) {
		this.Lock()
		defer this.Unlock()
		this.session = nil
	})

	this.session.Start(func(event *kendynet.Event) {
		if event.EventType == kendynet.EventTypeError {
			event.Session.Close(event.Data.(error).Error(), 0)
		} else {
			this.proxy.respChan <- event.Data.(*kendynet.ByteBuffer)
		}
	})

	now := time.Now()

	for _, v := range this.pendingSend {
		if !now.After(v.sendDeadline) {
			this.session.SendMessage(v.msg)
		} else {
			Infoln("sendDeadline")
		}
	}

	this.pendingSend = []*pendingMsg{}

	this.Unlock()
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
				time.Sleep(1 * time.Second)
			}
		}
	}()
}

func (this *Conn) SendReq(sendDeadline time.Time, req *kendynet.ByteBuffer) error {
	this.Lock()
	defer this.Unlock()
	if nil != this.session {
		return this.session.SendMessage(req)
	} else {
		var err error

		if len(this.pendingSend) >= maxSendQueueSize {
			err = kendynet.ErrSendQueFull
		} else {
			this.pendingSend = append(this.pendingSend, &pendingMsg{
				sendDeadline: sendDeadline,
				msg:          req,
			})
		}

		this.dial()

		return err
	}
}
