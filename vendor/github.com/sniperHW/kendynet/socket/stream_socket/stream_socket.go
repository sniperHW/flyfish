/*
*  tcp或unix域套接字会话
 */

package stream_socket

import (
	"net"
	"sync"
	"time"
	"bufio"
	"io"
	"github.com/sniperHW/kendynet"
	"github.com/sniperHW/kendynet/util"
)

const (
	started = (1 << 0)
	closed  = (1 << 1)
	wclosed = (1 << 2)
	rclosed = (1 << 3)
)

type StreamSocket struct {
	conn          net.Conn
	ud            interface{}
	sendQue       *util.BlockQueue
	receiver      kendynet.Receiver
	encoder       kendynet.EnCoder
	flag          int32
	SendTimeout   time.Duration
	RecvTimeout   time.Duration
	mutex         sync.Mutex
	onClose       func(kendynet.StreamSession, string)
	onEvent       func(*kendynet.Event)
	closeReason   string
	sendCloseChan chan int
	WantSend      uint64
	Sended        uint64
	nextShow      int64
}

func (this *StreamSocket) SetUserData(ud interface{}) {
	this.mutex.Lock()
	defer this.mutex.Unlock()
	this.ud = ud
}

func (this *StreamSocket) GetUserData() (ud interface{}) {
	this.mutex.Lock()
	defer this.mutex.Unlock()
	ud = this.ud
	return this.ud
}

func (this *StreamSocket) LocalAddr() net.Addr {
	return this.conn.LocalAddr()
}

func (this *StreamSocket) RemoteAddr() net.Addr {
	return this.conn.RemoteAddr()
}

func (this *StreamSocket) isClosed() (ret bool) {
	this.mutex.Lock()
	ret = (this.flag & closed) > 0
	this.mutex.Unlock()
	return
}

func (this *StreamSocket) doClose() {
	this.conn.Close()
	this.mutex.Lock()
	onClose := this.onClose
	this.mutex.Unlock()
	if nil != onClose {
		onClose(this, this.closeReason)
	}
}

func (this *StreamSocket) shutdownRead() {
	switch this.conn.(type) {
	case *net.TCPConn:
		this.conn.(*net.TCPConn).CloseRead()
		break
	case *net.UnixConn:
		this.conn.(*net.UnixConn).CloseRead()
		break
	}
}

func (this *StreamSocket) Close(reason string, delay time.Duration) {
	this.mutex.Lock()
	if (this.flag & closed) > 0 {
		this.mutex.Unlock()
		return
	}

	this.closeReason = reason
	this.flag |= (closed | rclosed)
	if this.flag&wclosed > 0 {
		delay = 0 //写端已经关闭，delay参数没有意义设置为0
	}

	this.sendQue.Close()
	this.mutex.Unlock()
	if this.sendQue.Len() > 0 {
		delay = delay * time.Second
		if delay <= 0 {
			this.sendQue.Clear()
		}
	}
	if delay > 0 {
		this.shutdownRead()
		ticker := time.NewTicker(delay)
		go func() {
			/*
			 *	delay > 0,sendThread最多需要经过delay秒之后才会结束，
			 *	为了避免阻塞调用Close的goroutine,启动一个新的goroutine在chan上等待事件
			 */
			select {
			case <-this.sendCloseChan:
			case <-ticker.C:
			}
			ticker.Stop()
			this.doClose()
		}()
	} else {
		this.doClose()
	}

}

func (this *StreamSocket) SetCloseCallBack(cb func(kendynet.StreamSession, string)) {
	this.mutex.Lock()
	defer this.mutex.Unlock()
	this.onClose = cb
}

func (this *StreamSocket) SetEncoder(encoder kendynet.EnCoder) {
	this.mutex.Lock()
	defer this.mutex.Unlock()
	this.encoder = encoder
}

func (this *StreamSocket) SetReceiver(r kendynet.Receiver) {
	this.mutex.Lock()
	defer this.mutex.Unlock()
	if (this.flag & started) > 0 {
		return
	}
	this.receiver = r
}

func (this *StreamSocket) sendMessage(msg kendynet.Message) error {
	if msg == nil {
		return kendynet.ErrInvaildBuff
	} else if (this.flag&closed) > 0 || (this.flag&wclosed) > 0 {
		return kendynet.ErrSocketClose
	} else {
		//this.flush()
		if nil != this.sendQue.Add(msg) {
			return kendynet.ErrSocketClose
		}
	}
	return nil
}

func (this *StreamSocket) Send(o interface{}) error {
	if o == nil {
		return kendynet.ErrInvaildObject
	}

	this.mutex.Lock()
	defer this.mutex.Unlock()

	if this.encoder == nil {
		return kendynet.ErrInvaildEncoder
	}

	msg, err := this.encoder.EnCode(o)

	if err != nil {
		return err
	}

	return this.sendMessage(msg)
}

func (this *StreamSocket) SendMessage(msg kendynet.Message) error {
	this.mutex.Lock()
	defer this.mutex.Unlock()
	return this.sendMessage(msg)
}

func recvThreadFunc(session *StreamSocket) {

	for !session.isClosed() {

		var p interface{}
		var err error

		recvTimeout := session.RecvTimeout

		if recvTimeout > 0 {
			session.conn.SetReadDeadline(time.Now().Add(recvTimeout))
			p, err = session.receiver.ReceiveAndUnpack(session)
			session.conn.SetReadDeadline(time.Time{})
		} else {
			p, err = session.receiver.ReceiveAndUnpack(session)
		}

		if session.isClosed() {
			//上层已经调用关闭，所有事件都不再传递上去
			break
		}
		if err != nil || p != nil {
			var event kendynet.Event
			event.Session = session
			if err != nil {
				event.EventType = kendynet.EventTypeError
				event.Data = err
				session.mutex.Lock()
				if err == io.EOF {
					session.flag |= rclosed
				} else if !kendynet.IsNetTimeout(err) {
					kendynet.Errorf("ReceiveAndUnpack error:%s\n", err.Error())
					session.flag |= (rclosed | wclosed)
				}
				session.mutex.Unlock()
			} else {
				event.EventType = kendynet.EventTypeMessage
				event.Data = p
			}
			/*出现错误不主动退出循环，除非用户调用了session.Close()
			 * 避免用户遗漏调用Close(不调用Close会持续通告错误)
			 */
			session.onEvent(&event)
			if session.isClosed() {
				break
			}
		}
	}
}


func sendThreadFunc(session *StreamSocket) {

	var err error

	defer func(){
		session.sendCloseChan <- 1
	}()

	writer := bufio.NewWriterSize(session.conn,65535*2)

	for {
		closed,localList := session.sendQue.Get()
		size := len(localList)
		if closed && size == 0 {
			break
		}

		for i := 0; i < size; i++ {
			msg := localList[i].(kendynet.Message)



			data := msg.Bytes()
			for data != nil || (i == (size - 1) && writer.Buffered() > 0) {
				if data != nil {
					var s int
					if len(data) > writer.Available() {
						s = writer.Available()
					} else {
						s = len(data)
					}
					writer.Write(data[:s])

					if s != len(data) {
						data = data[s:]
						//kendynet.Errorln("s != len(data)")
					} else {
						data = nil
					}
				}

				if writer.Available() == 0 || i == (size - 1) {

					timeout := session.SendTimeout
					if timeout > 0 {
						session.conn.SetWriteDeadline(time.Now().Add(timeout))
						err = writer.Flush()
						session.conn.SetWriteDeadline(time.Time{})
					} else {
						err = writer.Flush()
					}
					if err != nil && err != io.ErrShortWrite {
						if session.sendQue.Closed() {
							return
						}
						if kendynet.IsNetTimeout(err) {
							err = kendynet.ErrSendTimeout
						} else {
							kendynet.Errorf("writer.Flush error:%s\n",err.Error())
							session.mutex.Lock()
							session.flag |= wclosed
							session.mutex.Unlock()
						}
						event := &kendynet.Event{Session:session,EventType:kendynet.EventTypeError,Data:err}
						session.onEvent(event)
						if session.sendQue.Closed() {
							return
						}
					}
				}
			}
		}
	}
}
/*

type sendBuffer struct {
	b       int
	e       int
	bytes   []byte
	session *StreamSocket
}

func (this *sendBuffer) expand(need int, final bool) {
	total := this.e + need
	var newBuff []byte
	if final {
		newBuff = make([]byte, total)
	} else {
		newBuff = make([]byte, total*2)
	}

	copy(newBuff, this.bytes[:this.e])
	this.bytes = newBuff
}

func (this *sendBuffer) append(data []byte, final bool) {
	capRemain := cap(this.bytes) - this.e

	if capRemain < len(data) {
		this.expand(len(data), final)
	}
	copy(this.bytes[this.e:], data)
	this.e += len(data)
}

func (this *sendBuffer) flush() error {
	if this.e > 0 {
		n, err := this.session.conn.Write(this.bytes[this.b:this.e])
		if n > 0 {
			this.b += n
			if this.b >= this.e {
				this.e = 0
				this.b = 0
				if cap(this.bytes) > 65535*2 {
					this.bytes = make([]byte, 65535)
				}
			}
		}
		return err
	} else {
		return nil
	}
}

func (this *sendBuffer) empty() bool {
	return this.e == 0
}

func sendThreadFunc(session *StreamSocket) {

	var err error

	defer func() {
		session.sendCloseChan <- 1
	}()

	buffer := &sendBuffer{
		session: session,
		bytes:   make([]byte, 65535),
	}

	for {
		closed, localList := session.sendQue.Get()
		size := len(localList)
		if closed && size == 0 {
			break
		}

		for i := 0; i < size; i++ {
			msg := localList[i].(kendynet.Message)
			//session.WantSend += uint64(len(msg.Bytes()))
			buffer.append(msg.Bytes(), (i == size-1))
		}

		for !buffer.empty() {
			timeout := session.SendTimeout
			if timeout > 0 {
				session.conn.SetWriteDeadline(time.Now().Add(timeout))
				err = buffer.flush()
				session.conn.SetWriteDeadline(time.Time{})
			} else {
				err = buffer.flush()
			}

			if err != nil && err != io.ErrShortWrite {
				if session.sendQue.Closed() {
					return
				}
				if kendynet.IsNetTimeout(err) {
					err = kendynet.ErrSendTimeout
				} else {
					kendynet.Errorf("writer.Flush error:%s\n", err.Error())
					session.mutex.Lock()
					session.flag |= wclosed
					session.mutex.Unlock()
				}
				event := &kendynet.Event{Session: session, EventType: kendynet.EventTypeError, Data: err}
				session.onEvent(event)
				if session.sendQue.Closed() {
					return
				}
			}
		}
	}
}
*/

func (this *StreamSocket) Start(eventCB func(*kendynet.Event)) error {

	this.mutex.Lock()
	defer this.mutex.Unlock()

	if (this.flag & closed) > 0 {
		return kendynet.ErrSocketClose
	}

	if (this.flag & started) > 0 {
		return kendynet.ErrStarted
	}

	if eventCB == nil {
		return kendynet.ErrNoOnEvent
	}

	if this.receiver == nil {
		return kendynet.ErrNoReceiver
	}

	this.onEvent = eventCB
	this.flag |= started
	go sendThreadFunc(this)
	go recvThreadFunc(this)
	return nil
}

func NewStreamSocket(conn net.Conn) kendynet.StreamSession {
	if nil == conn {
		return nil
	} else {
		switch conn.(type) {
		case *net.TCPConn:
			break
		case *net.UnixConn:
			break
		default:
			kendynet.Errorf("NewStreamSocket() invaild conn type\n")
			return nil
		}

		return &StreamSocket{
			conn:          conn,
			sendQue:       util.NewBlockQueue(),
			sendCloseChan: make(chan int, 1),
		}
	}
}

func (this *StreamSocket) GetUnderConn() interface{} {
	return this.conn
}

func (this *StreamSocket) Read(b []byte) (int, error) {
	return this.conn.Read(b)
}

func (this *StreamSocket) SetRecvTimeout(timeout time.Duration) {
	this.RecvTimeout = timeout * time.Millisecond
}

func (this *StreamSocket) SetSendTimeout(timeout time.Duration) {
	this.SendTimeout = timeout * time.Millisecond
}
