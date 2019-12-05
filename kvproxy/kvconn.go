package kvproxy

import (
	"encoding/binary"
	"github.com/golang/protobuf/proto"
	protocol "github.com/sniperHW/flyfish/proto"
	"github.com/sniperHW/kendynet"
	connector "github.com/sniperHW/kendynet/socket/connector/tcp"
	"net"
	"sync"
	"time"
)

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

func sendLoginReq(session kendynet.StreamSession, loginReq *protocol.LoginReq) bool {
	conn := session.GetUnderConn().(*net.TCPConn)
	buffer := kendynet.NewByteBuffer(64)
	data, _ := proto.Marshal(loginReq)
	buffer.AppendUint16(uint16(len(data)))
	buffer.AppendBytes(data)

	conn.SetWriteDeadline(time.Now().Add(time.Second * 5))
	_, err := conn.Write(buffer.Bytes())
	conn.SetWriteDeadline(time.Time{})
	return nil == err
}

func recvLoginResp(session kendynet.StreamSession) (*protocol.LoginResp, error) {
	conn := session.GetUnderConn().(*net.TCPConn)
	buffer := make([]byte, 1024)
	w := 0
	pbsize := 0
	for {
		conn.SetReadDeadline(time.Now().Add(time.Second * 5))
		n, err := conn.Read(buffer[w:])
		conn.SetReadDeadline(time.Time{})

		if nil != err {
			return nil, err
		}

		w = w + n

		if w >= 2 {
			pbsize = int(binary.BigEndian.Uint16(buffer[:2]))
		}

		if w >= pbsize+2 {
			loginResp := &protocol.LoginResp{}
			if err = proto.Unmarshal(buffer[2:w], loginResp); err != nil {
				return nil, err
			} else {
				return loginResp, nil
			}
		}
	}
}

func (this *Conn) onConnected(session kendynet.StreamSession) {

	defer func() {
		this.Lock()
		this.dialing = false
		this.Unlock()
	}()

	loginReq := &protocol.LoginReq{Compress: true}
	if !sendLoginReq(session, loginReq) {
		session.Close("login failed", 0)
		return
	}

	loginResp, err := recvLoginResp(session)
	if nil != err || !loginResp.GetOk() {
		session.Close("login failed", 0)
		return
	}

	this.Lock()
	this.session = session

	//this.session.SetReceiver(codec.NewReceiver(pb.GetNamespace("response"), loginResp.GetCompress()))

	//this.session.SetEncoder(codec.NewEncoder(pb.GetNamespace("request"), loginResp.GetCompress()))

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
			this.proxy.onResp(event.Data.(*kendynet.ByteBuffer))
		}
	})

	now := time.Now()

	for _, v := range this.pendingSend {
		if !now.After(v.sendDeadline) {
			this.session.SendMessage(v.msg)
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
		this.pendingSend = append(this.pendingSend, &pendingMsg{
			sendDeadline: sendDeadline,
			msg:          req,
		})
		this.dial()
		return nil
	}
}
