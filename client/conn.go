package client

import (
	"encoding/binary"
	"github.com/golang/protobuf/proto"
	"github.com/sniperHW/flyfish/codec"
	"github.com/sniperHW/flyfish/codec/pb"
	"github.com/sniperHW/flyfish/errcode"
	protocol "github.com/sniperHW/flyfish/proto"
	"github.com/sniperHW/kendynet"
	"github.com/sniperHW/kendynet/event"
	connector "github.com/sniperHW/kendynet/socket/connector/tcp"
	"github.com/sniperHW/kendynet/timer"
	"github.com/sniperHW/kendynet/util"
	"net"
	"time"
)

const maxPendingSize int = 10000

type Conn struct {
	session     kendynet.StreamSession
	seqno       int64
	addr        string
	minheap     *util.MinHeap         //超时小根堆
	pendingSend []*cmdContext         //等待发送的消息
	waitResp    map[int64]*cmdContext //等待响应的消息
	eventQueue  *event.EventQueue     //此客户端的主处理队列
	dialing     bool
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
	go c.eventQueue.Run()
	timer.Repeat(time.Duration(100)*time.Millisecond, c.eventQueue, func(_ *timer.Timer) {
		now := time.Now()
		c.checkTimeout(&now)
	})
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
				this.c.doCallBack(c.cb, errcode.ERR_TIMEOUT)
			} else {

				seqno := c.req.GetHead().Seqno

				if _, ok := this.waitResp[seqno]; ok {
					delete(this.waitResp, seqno)
					this.c.doCallBack(c.cb, errcode.ERR_TIMEOUT)
				}
			}
		} else {
			break
		}
	}
}

/*func (this *Conn) ping(now *time.Time) {
	if nil != this.session && now.After(this.nextPing) {
		this.nextPing = now.Add(protocol.PingTime)
		req := codec.NewMessage(codec.CommonHead{}, &protocol.PingReq{
			Timestamp: now.UnixNano(),
		})
		this.session.Send(req)
	}
}*/

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

	loginReq := &protocol.LoginReq{Compress: this.c.compress}
	if !sendLoginReq(session, loginReq) {
		session.Close("login failed", 0)
		this.eventQueue.Post(func() {
			this.dialing = false
			this.dial()
		})
		return
	}

	loginResp, err := recvLoginResp(session)
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
			this.c.doCallBack(c.cb, errcode.ERR_CONNECTION)
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
				Errorln("dial error", this.addr, err)
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
			this.c.doCallBack(c.cb, errcode.ERR_BUSY)
		} else {
			this.c.doCallBack(c.cb, errcode.ERR_CONNECTION)
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
				this.c.doCallBack(c.cb, errcode.ERR_BUSY)
			}
		} else {
			this.minheap.Insert(c)
			this.sendReq(c)
		}
	})
}
