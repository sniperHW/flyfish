package kvproxy

import (
	flyfish_logger "github.com/sniperHW/flyfish/logger"
	"github.com/sniperHW/flyfish/net"
	protocol "github.com/sniperHW/flyfish/proto"
	"github.com/sniperHW/kendynet"
	"github.com/sniperHW/kendynet/timer"
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
	timer       *timer.Timer
	nextPing    time.Time
}

func openConn(proxy *kvproxy, serverID int, addr string, compress bool) *Conn {
	c := &Conn{
		addr:        addr,
		serverID:    serverID,
		pendingSend: []*pendingMsg{},
		compress:    compress,
		proxy:       proxy,
		nextPing:    time.Now().Add(protocol.PingTime),
	}
	return c
}

func (this *Conn) Close() {
	this.Lock()
	this.pendingSend = []*pendingMsg{}
	if nil != this.session {
		this.Unlock()
		this.session.Close(nil, 0)
	} else {
		this.Unlock()
	}
}

func (this *Conn) onConnected(session kendynet.StreamSession) {
	this.Lock()
	this.dialing = false
	this.session = session
	session.SetRecvTimeout(protocol.PingTime * 2)
	this.session.SetSendQueueSize(maxSendQueueSize)
	this.session.SetInBoundProcessor(NewReceiver())

	this.session.SetCloseCallBack(func(sess kendynet.StreamSession, reason error) {
		this.Lock()
		defer this.Unlock()
		this.session = nil
		if nil != this.timer {
			this.timer.Cancel()
			this.timer = nil
		}
	})

	this.session.BeginRecv(func(s kendynet.StreamSession, msg interface{}) {
		this.proxy.respChan <- msg.(*kendynet.ByteBuffer)
	})

	now := time.Now()

	for _, v := range this.pendingSend {
		if !now.After(v.sendDeadline) {
			this.session.SendMessage(v.msg)
		} else {
			flyfish_logger.GetSugar().Info("sendDeadline")
		}
	}

	this.pendingSend = []*pendingMsg{}

	//不主动发心跳，如果空闲就让服务器释放连接
	/*this.timer = timer.Repeat(time.Second, nil, func(t *timer.Timer, _ interface{}) {
		now := time.Now()
		this.nextPing = now.Add(protocol.PingTime)
		req := codec.NewMessage(codec.CommonHead{}, &protocol.PingReq{
			Timestamp: now.UnixNano(),
		})
		session.Send(req)
	}, nil)*/

	this.Unlock()

}

func (this *Conn) dial() {
	if this.dialing {
		return
	}

	this.dialing = true

	go func() {
		c := net.NewConnector("tcp", this.addr, this.compress)
		for {
			session, _, err := c.Dial(time.Second * 5)
			if nil == err {
				this.onConnected(session)
				return
			} else {
				flyfish_logger.GetSugar().Errorf("dial error %s %v", this.addr, err)
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
