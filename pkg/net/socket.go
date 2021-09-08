package net

import (
	"errors"
	"github.com/sniperHW/flyfish/pkg/buffer"
	"net"
	"runtime"
	"sync/atomic"
	"time"
)

func isNetTimeout(err error) bool {
	switch err.(type) {
	case net.Error:
		if err.(net.Error).Timeout() {
			return true
		}
	default:
	}
	return false
}

const (
	fclosed  = int32(1 << 1)
	frclosed = int32(1 << 2)
	fwclosed = int32(1 << 3)
	fdoclose = int32(1 << 4)
)

type Socket struct {
	conn             net.Conn
	flag             int32
	ud               atomic.Value
	sendCloseChan    chan struct{}
	closeOnce        int32
	beginOnce        int32
	sendOnce         int32
	doCloseOnce      int32
	encoder          Encoder
	inboundProcessor InBoundProcessor
	errorCallback    func(*Socket, error)
	closeCallBack    func(*Socket, error)
	inboundCallBack  func(*Socket, interface{})
	ioCount          int32
	closeReason      error
	sendTimeout      int64
	recvTimeout      int64
	sendQueue        *SendQueue
}

func (s *Socket) setFlag(flag int32) {
	for {
		f := atomic.LoadInt32(&s.flag)
		if atomic.CompareAndSwapInt32(&s.flag, f, f|flag) {
			break
		}
	}
}

func (s *Socket) testFlag(flag int32) bool {
	return atomic.LoadInt32(&s.flag)&flag > 0
}

func (s *Socket) IsClosed() bool {
	return s.testFlag(fclosed)
}

func (s *Socket) LocalAddr() net.Addr {
	return s.conn.LocalAddr()
}

func (s *Socket) RemoteAddr() net.Addr {
	return s.conn.RemoteAddr()
}

func (s *Socket) SetUserData(ud interface{}) *Socket {
	s.ud.Store(ud)
	return s
}

func (s *Socket) GetUserData() interface{} {
	return s.ud.Load()
}

func (s *Socket) GetNetConn() net.Conn {
	return s.conn
}

func (s *Socket) SetErrorCallBack(cb func(*Socket, error)) *Socket {
	s.errorCallback = cb
	return s
}

func (s *Socket) SetCloseCallBack(cb func(*Socket, error)) *Socket {
	s.closeCallBack = cb
	return s
}

func (s *Socket) SetEncoder(encoder Encoder) *Socket {
	s.encoder = encoder
	return s
}

func (s *Socket) SetInBoundProcessor(in InBoundProcessor) *Socket {
	s.inboundProcessor = in
	return s
}

func (s *Socket) ShutdownRead() {
	s.setFlag(frclosed)
	s.conn.(interface{ CloseRead() error }).CloseRead()
}

func (s *Socket) addIO() {
	atomic.AddInt32(&s.ioCount, 1)
}

func (s *Socket) SetSendQueueSize(size int) *Socket {
	s.sendQueue.SetFullSize(size)
	return s
}

func (s *Socket) SetRecvTimeout(timeout time.Duration) *Socket {
	atomic.StoreInt64(&s.recvTimeout, int64(timeout))
	return s
}

func (s *Socket) SetSendTimeout(timeout time.Duration) *Socket {
	atomic.StoreInt64(&s.sendTimeout, int64(timeout))
	return s
}

func (s *Socket) getRecvTimeout() time.Duration {
	return time.Duration(atomic.LoadInt64(&s.recvTimeout))
}

func (s *Socket) getSendTimeout() time.Duration {
	return time.Duration(atomic.LoadInt64(&s.sendTimeout))
}

func (this *Socket) ShutdownWrite() {
	closeOK, remain := this.sendQueue.Close()
	if closeOK && remain == 0 {
		this.conn.(interface{ CloseWrite() error }).CloseWrite()
	}
}

func (this *Socket) recvThreadFunc() {
	defer this.ioDone()

	oldTimeout := this.getRecvTimeout()
	timeout := oldTimeout

	for !this.testFlag(fclosed | frclosed) {

		var (
			p   interface{}
			err error
			n   int
		)

		isUnpackError := false

		for {
			p, err = this.inboundProcessor.Unpack()
			if nil != p {
				break
			} else if nil != err {
				isUnpackError = true
				break
			} else {

				oldTimeout = timeout
				timeout = this.getRecvTimeout()

				if oldTimeout != timeout && timeout == 0 {
					this.conn.SetReadDeadline(time.Time{})
				}

				buff := this.inboundProcessor.GetRecvBuff()
				if timeout > 0 {
					this.conn.SetReadDeadline(time.Now().Add(timeout))
					n, err = this.conn.Read(buff)
				} else {
					n, err = this.conn.Read(buff)
				}

				if nil == err {
					this.inboundProcessor.OnData(buff[:n])
				} else {
					break
				}
			}
		}

		if !this.testFlag(fclosed | frclosed) {
			if nil != err {

				if isNetTimeout(err) {
					err = ErrRecvTimeout
				}

				if nil != this.errorCallback {

					if isUnpackError {
						this.Close(err, 0)
					} else if err != ErrRecvTimeout {
						this.setFlag(frclosed)
					}

					this.errorCallback(this, err)
				} else {
					this.Close(err, 0)
				}

			} else if p != nil {
				this.inboundCallBack(this, p)
			}
		} else {
			break
		}
	}
}

func (this *Socket) sendThreadFunc() {
	defer func() {
		close(this.sendCloseChan)
		this.ioDone()
	}()

	var err error

	localList := make([]interface{}, 0, 32)

	closed := false

	var n int

	oldTimeout := this.getSendTimeout()
	timeout := oldTimeout

	for {

		closed, localList = this.sendQueue.Get(localList)
		size := len(localList)
		if closed && size == 0 {
			this.conn.(interface{ CloseWrite() error }).CloseWrite()
			break
		}

		b := buffer.Get()
		for i := 0; i < size; {
			if b.Len() == 0 {
				for i < size {
					l := b.Len()
					err = this.encoder.EnCode(localList[i], b)
					localList[i] = nil
					i++
					if nil != err {
						//EnCode错误，这个包已经写入到b中的内容需要直接丢弃
						b.SetLen(l)
						GetSugar().Errorf("encode error:%v", err)
					}
				}
			}

			if b.Len() == 0 {
				b.Free()
				break
			}

			oldTimeout = timeout
			timeout = this.getSendTimeout()

			if oldTimeout != timeout && timeout == 0 {
				this.conn.SetWriteDeadline(time.Time{})
			}

			if timeout > 0 {
				this.conn.SetWriteDeadline(time.Now().Add(timeout))
				n, err = this.conn.Write(b.Bytes())
			} else {
				n, err = this.conn.Write(b.Bytes())
			}

			if nil == err {
				b.Reset()
			} else if !this.testFlag(fclosed) {
				if isNetTimeout(err) {
					err = ErrSendTimeout
				} else {
					this.Close(err, 0)
				}

				if nil != this.errorCallback {
					this.errorCallback(this, err)
				}

				if this.testFlag(fclosed) {
					b.Free()
					return
				} else {
					//超时可能完成部分发送，将已经发送部分丢弃
					b.DropFirstNBytes(n)
				}
			} else {
				b.Free()
				return
			}
		}
	}
}

func (this *Socket) BeginRecv(cb func(*Socket, interface{})) (err error) {

	if atomic.CompareAndSwapInt32(&this.beginOnce, 0, 1) {
		if nil == cb {
			err = errors.New("BeginRecv cb is nil")
			return
		}

		if nil == this.inboundProcessor {
			err = errors.New("inboundProcessor is nil")
			return
		}

		if this.testFlag(fclosed | frclosed) {
			err = ErrSocketClose
		} else {
			this.inboundCallBack = cb
			this.addIO()
			go this.recvThreadFunc()
		}
	}

	return
}

func (this *Socket) Send(o interface{}) (err error) {
	if nil == o {
		return errors.New("o == nil")
	}
	err = this.sendQueue.Add(o)
	if err == ErrQueueClosed {
		err = ErrSocketClose
	} else if err == ErrQueueFull {
		err = ErrSendQueFull
	} else {
		if atomic.CompareAndSwapInt32(&this.sendOnce, 0, 1) {
			this.addIO()
			go this.sendThreadFunc()
		}
	}
	return
}

func (this *Socket) ioDone() {
	if 0 == atomic.AddInt32(&this.ioCount, -1) && this.testFlag(fdoclose) {
		if atomic.CompareAndSwapInt32(&this.doCloseOnce, 0, 1) {
			if nil != this.closeCallBack {
				this.closeCallBack(this, this.closeReason)
			}
		}
	}
}

func (this *Socket) Close(reason error, delay time.Duration) {

	if atomic.CompareAndSwapInt32(&this.closeOnce, 0, 1) {
		runtime.SetFinalizer(this, nil)

		this.setFlag(fclosed)

		_, remain := this.sendQueue.Close()

		if remain > 0 && delay > 0 {
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
				this.conn.Close()
			}()

		} else {
			this.conn.Close()
		}

		this.closeReason = reason
		this.setFlag(fdoclose)

		if atomic.LoadInt32(&this.ioCount) == 0 {
			if atomic.CompareAndSwapInt32(&this.doCloseOnce, 0, 1) {
				if nil != this.closeCallBack {
					this.closeCallBack(this, reason)
				}
			}
		}

	}
}

var DefaultSendQueSize int = 256

func NewSocket(conn net.Conn) *Socket {

	switch conn.(type) {
	case *net.TCPConn, *net.UnixConn:
		break
	default:
		return nil
	}

	s := &Socket{
		conn:          conn,
		sendCloseChan: make(chan struct{}),
		sendQueue:     NewSendQueue(DefaultSendQueSize),
	}

	runtime.SetFinalizer(s, func(s *Socket) {
		s.Close(errors.New("gc"), 0)
	})

	return s
}
