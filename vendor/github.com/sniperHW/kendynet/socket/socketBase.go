package socket

import (
	"github.com/sniperHW/kendynet"
	"github.com/sniperHW/kendynet/util"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

const (
	started = (1 << 0)
	closed  = (1 << 1)
	wclosed = (1 << 2)
	rclosed = (1 << 3)
)

type SocketImpl interface {
	kendynet.StreamSession
	recvThreadFunc()
	sendThreadFunc()
	getNetConn() net.Conn
	sendMessage(kendynet.Message) error
	defaultReceiver() kendynet.Receiver
}

type SocketBase struct {
	ud          interface{}
	sendQue     *util.BlockQueue
	receiver    kendynet.Receiver
	encoder     *kendynet.EnCoder
	flag        int32
	sendTimeout atomic.Value
	recvTimeout atomic.Value //time.Duration
	//waitMode      atomic.Value
	mutex         sync.Mutex
	onClose       func(kendynet.StreamSession, string)
	onEvent       func(*kendynet.Event)
	closeReason   string
	sendCloseChan chan struct{}
	imp           SocketImpl
}

func (this *SocketBase) IsClosed() bool {
	this.mutex.Lock()
	defer this.mutex.Unlock()
	return this.flag&closed > 0
}

func (this *SocketBase) LocalAddr() net.Addr {
	return this.imp.getNetConn().LocalAddr()
}

func (this *SocketBase) RemoteAddr() net.Addr {
	return this.imp.getNetConn().RemoteAddr()
}

func (this *SocketBase) SetUserData(ud interface{}) {
	this.mutex.Lock()
	defer this.mutex.Unlock()
	this.ud = ud
}

func (this *SocketBase) GetUserData() (ud interface{}) {
	this.mutex.Lock()
	defer this.mutex.Unlock()
	return this.ud
}

func (this *SocketBase) doClose() {
	this.imp.getNetConn().Close()
	this.mutex.Lock()
	onClose := this.onClose
	this.mutex.Unlock()
	if nil != onClose {
		onClose(this.imp.(kendynet.StreamSession), this.closeReason)
	}
}

func (this *SocketBase) shutdownRead() {
	underConn := this.imp.getNetConn()
	switch underConn.(type) {
	case *net.TCPConn:
		underConn.(*net.TCPConn).CloseRead()
		break
	case *net.UnixConn:
		underConn.(*net.UnixConn).CloseRead()
		break
	}
}

func (this *SocketBase) ShutdownRead() {
	this.mutex.Lock()
	defer this.mutex.Unlock()
	if (this.flag & closed) > 0 {
		return
	}
	this.flag |= rclosed
	this.shutdownRead()
}

func (this *SocketBase) Start(eventCB func(*kendynet.Event)) error {

	if eventCB == nil {
		panic("eventCB == nil")
	}

	this.mutex.Lock()
	defer this.mutex.Unlock()

	if (this.flag & closed) > 0 {
		return kendynet.ErrSocketClose
	}

	if (this.flag & started) > 0 {
		return kendynet.ErrStarted
	}

	if this.receiver == nil {
		this.receiver = this.imp.defaultReceiver()
	}

	this.onEvent = eventCB
	this.flag |= started

	go this.imp.sendThreadFunc()
	go this.imp.recvThreadFunc()
	return nil
}

func (this *SocketBase) SetRecvTimeout(timeout time.Duration) {
	this.recvTimeout.Store(timeout)
}

func (this *SocketBase) SetSendTimeout(timeout time.Duration) {
	this.sendTimeout.Store(timeout)
}

func (this *SocketBase) SetCloseCallBack(cb func(kendynet.StreamSession, string)) {
	this.mutex.Lock()
	defer this.mutex.Unlock()
	this.onClose = cb
}

func (this *SocketBase) SetEncoder(encoder kendynet.EnCoder) {
	atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&this.encoder)), unsafe.Pointer(&encoder))
}

func (this *SocketBase) SetReceiver(r kendynet.Receiver) {
	this.mutex.Lock()
	defer this.mutex.Unlock()
	if (this.flag & started) > 0 {
		return
	}
	this.receiver = r
}

func (this *SocketBase) SetSendQueueSize(size int) {
	this.sendQue.SetFullSize(size)
}

func (this *SocketBase) Send(o interface{}) error {
	if o == nil {
		return kendynet.ErrInvaildObject
	}

	encoder := (*kendynet.EnCoder)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&this.encoder))))

	if nil == *encoder {
		return kendynet.ErrInvaildEncoder
	}

	msg, err := (*encoder).EnCode(o)

	if err != nil {
		return err
	}

	this.mutex.Lock()
	err = this.imp.sendMessage(msg)
	this.mutex.Unlock()
	return err
}

func (this *SocketBase) SendMessage(msg kendynet.Message) error {
	this.mutex.Lock()
	err := this.imp.sendMessage(msg)
	this.mutex.Unlock()
	return err
}

func (this *SocketBase) recvThreadFunc() {

	conn := this.imp.getNetConn()

	for !this.IsClosed() {

		var (
			p     interface{}
			err   error
			event kendynet.Event
		)

		recvTimeout := this.getRecvTimeout()

		if recvTimeout > 0 {
			conn.SetReadDeadline(time.Now().Add(recvTimeout))
			p, err = this.receiver.ReceiveAndUnpack(this.imp)
			conn.SetReadDeadline(time.Time{})
		} else {
			p, err = this.receiver.ReceiveAndUnpack(this.imp)
		}

		if this.IsClosed() {
			//上层已经调用关闭，所有事件都不再传递上去
			break
		}
		if err != nil || p != nil {
			event.Session = this.imp
			if err != nil {
				event.EventType = kendynet.EventTypeError
				event.Data = err
				this.mutex.Lock()
				if err == io.EOF {
					this.flag |= rclosed
				} else if kendynet.IsNetTimeout(err) {
					event.Data = kendynet.ErrRecvTimeout
				} else {
					kendynet.GetLogger().Errorf("ReceiveAndUnpack error:%s\n", err.Error())
					this.flag |= (rclosed | wclosed)
				}
				this.mutex.Unlock()
			} else {
				event.EventType = kendynet.EventTypeMessage
				event.Data = p
			}

			/*出现错误不主动退出循环，除非用户调用了session.Close()
			 * 避免用户遗漏调用Close(不调用Close会持续通告错误)
			 */

			//if this.isWaitMode() {
			//	event.EventWaiter = kendynet.NewEventWaiter()
			//	this.onEvent(&event)
			//	event.EventWaiter.Wait()
			//} else {
			this.onEvent(&event)
			//}

		}
	}
}

/*
func (this *SocketBase) SetWaitMode(wait bool) {
	this.waitMode.Store(wait)
}

func (this *SocketBase) isWaitMode() bool {
	mode := this.waitMode.Load()
	if nil == mode {
		return false
	} else {
		return mode.(bool)
	}
}
*/

func (this *SocketBase) getRecvTimeout() time.Duration {
	t := this.recvTimeout.Load()
	if nil == t {
		return 0
	} else {
		return t.(time.Duration)
	}
}

func (this *SocketBase) getSendTimeout() time.Duration {
	t := this.sendTimeout.Load()
	if nil == t {
		return 0
	} else {
		return t.(time.Duration)
	}
}
