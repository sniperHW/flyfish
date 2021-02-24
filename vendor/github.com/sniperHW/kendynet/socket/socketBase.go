package socket

import (
	"github.com/sniperHW/kendynet"
	"github.com/sniperHW/kendynet/util"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

const (
	fclosed  = int32(1 << 1) //是否已经调用Close
	frclosed = int32(1 << 2) //调用来了shutdownRead
)

type SocketImpl interface {
	kendynet.StreamSession
	recvThreadFunc()
	sendThreadFunc()
	sendMessage(kendynet.Message) error
	defaultInBoundProcessor() kendynet.InBoundProcessor
}

type SocketBase struct {
	flag          int32
	ud            atomic.Value
	sendQue       *util.BlockQueue
	sendTimeout   atomic.Value
	recvTimeout   atomic.Value
	sendCloseChan chan struct{}
	imp           SocketImpl
	closeOnce     sync.Once
	beginOnce     sync.Once
	sendOnce      sync.Once
	ioWait        sync.WaitGroup

	encoder          kendynet.EnCoder
	inboundProcessor kendynet.InBoundProcessor
	errorCallback    func(kendynet.StreamSession, error)
	closeCallBack    func(kendynet.StreamSession, error)
	inboundCallBack  func(kendynet.StreamSession, interface{})
}

func (this *SocketBase) setFlag(flag int32) {
	for !atomic.CompareAndSwapInt32(&this.flag, this.flag, this.flag|flag) {
	}
}

func (this *SocketBase) testFlag(flag int32) bool {
	return atomic.LoadInt32(&this.flag)&flag > 0
}

func (this *SocketBase) IsClosed() bool {
	return this.testFlag(fclosed)
}

func (this *SocketBase) LocalAddr() net.Addr {
	return this.imp.GetNetConn().LocalAddr()
}

func (this *SocketBase) RemoteAddr() net.Addr {
	return this.imp.GetNetConn().RemoteAddr()
}

func (this *SocketBase) SetUserData(ud interface{}) kendynet.StreamSession {
	this.ud.Store(ud)
	return this.imp
}

func (this *SocketBase) GetUserData() interface{} {
	return this.ud.Load()
}

func (this *SocketBase) SetRecvTimeout(timeout time.Duration) kendynet.StreamSession {
	this.recvTimeout.Store(timeout)
	return this.imp
}

func (this *SocketBase) SetSendTimeout(timeout time.Duration) kendynet.StreamSession {
	this.sendTimeout.Store(timeout)
	return this.imp
}

func (this *SocketBase) SetErrorCallBack(cb func(kendynet.StreamSession, error)) kendynet.StreamSession {
	this.errorCallback = cb
	return this.imp
}

func (this *SocketBase) SetCloseCallBack(cb func(kendynet.StreamSession, error)) kendynet.StreamSession {
	this.closeCallBack = cb
	return this.imp
}

func (this *SocketBase) SetEncoder(encoder kendynet.EnCoder) kendynet.StreamSession {
	this.encoder = encoder
	return this.imp
}

func (this *SocketBase) SetInBoundProcessor(in kendynet.InBoundProcessor) kendynet.StreamSession {
	this.inboundProcessor = in
	return this.imp
}

func (this *SocketBase) SetSendQueueSize(size int) kendynet.StreamSession {
	this.sendQue.SetFullSize(size)
	return this.imp
}

func (this *SocketBase) ShutdownRead() {
	this.setFlag(frclosed)
	this.imp.GetNetConn().(interface{ CloseRead() error }).CloseRead()
}

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

func (this *SocketBase) Send(o interface{}) error {
	if this.encoder == nil {
		panic("Send s.encoder == nil")
	} else if nil == o {
		panic("Send o == nil")
	}

	msg, err := this.encoder.EnCode(o)

	if err != nil {
		return err
	}

	return this.imp.sendMessage(msg)
}

func (this *SocketBase) SendMessage(msg kendynet.Message) error {
	return this.imp.sendMessage(msg)
}

func (this *SocketBase) recvThreadFunc() {
	defer this.ioWait.Done()

	conn := this.imp.GetNetConn()

	for {
		var (
			p   interface{}
			err error
		)

		recvTimeout := this.getRecvTimeout()

		if recvTimeout > 0 {
			conn.SetReadDeadline(time.Now().Add(recvTimeout))
			p, err = this.inboundProcessor.ReceiveAndUnpack(this.imp)
			conn.SetReadDeadline(time.Time{})
		} else {
			p, err = this.inboundProcessor.ReceiveAndUnpack(this.imp)
		}

		if !this.testFlag(fclosed | frclosed) {
			if nil != err {
				if kendynet.IsNetTimeout(err) {
					err = kendynet.ErrRecvTimeout
				}

				if nil != this.errorCallback {
					if err != kendynet.ErrRecvTimeout {
						this.Close(err, 0)
					}
					this.errorCallback(this.imp, err)
				} else {
					this.Close(err, 0)
				}
			} else if p != nil {
				this.inboundCallBack(this.imp, p)
			}
		} else {
			break
		}
	}
}

func (this *SocketBase) BeginRecv(cb func(kendynet.StreamSession, interface{})) (err error) {

	this.beginOnce.Do(func() {
		if nil == cb {
			panic("BeginRecv cb is nil")
		}

		if this.testFlag(fclosed | frclosed) {
			err = kendynet.ErrSocketClose
		} else {
			if nil == this.inboundProcessor {
				this.inboundProcessor = this.imp.defaultInBoundProcessor()
			}
			this.inboundCallBack = cb
			this.ioWait.Add(1)
			go this.imp.recvThreadFunc()
		}
	})

	return
}

func (this *SocketBase) Close(reason error, delay time.Duration) {

	this.closeOnce.Do(func() {
		runtime.SetFinalizer(this.imp, nil)

		this.setFlag(fclosed)

		wclosed := this.sendQue.Closed()

		this.sendQue.Close()

		if wclosed || this.sendQue.Len() == 0 {
			delay = 0 //写端已经关闭，delay参数没有意义设置为0
		} else if delay > 0 {
			delay = delay * time.Second
		}

		if delay > 0 {
			this.ShutdownRead()
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
				this.imp.GetNetConn().Close()
			}()

		} else {
			this.sendQue.Clear()
			this.imp.GetNetConn().Close()
		}

		go func() {
			this.ioWait.Wait()
			if nil != this.closeCallBack {
				this.closeCallBack(this.imp, reason)
			}
		}()

	})

}
