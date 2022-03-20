package net

import (
	"errors"
	"github.com/sniperHW/flyfish/pkg/buffer"
	"math/rand"
	"net"
	"runtime"
	"sync"
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
)

type sendQueue struct {
	sync.Mutex
	ch     chan *buffer.Buffer
	closed bool
}

func (s *sendQueue) push(b *buffer.Buffer) error {
	s.Lock()
	defer s.Unlock()
	if !s.closed {
		s.ch <- b
		return nil
	} else {
		return errors.New("send queue closed")
	}
}

func (s *sendQueue) pop() (*buffer.Buffer, bool) {
	b, ok := <-s.ch
	return b, ok
}

func (s *sendQueue) close() int {
	s.Lock()
	defer s.Unlock()
	if !s.closed {
		s.closed = true
		close(s.ch)
	}
	return len(s.ch)
}

type OutputBufLimit struct {
	OutPutLimitSoft        int
	OutPutLimitSoftSeconds int
	OutPutLimitHard        int
}

var DefaultOutPutLimitSoft int = 128 * 1024      //128k
var DefaultOutPutLimitSoftSeconds int = 10       //10s
var DefaultOutPutLimitHard int = 4 * 1024 * 1024 //4M

type Socket struct {
	conn                     net.Conn
	flag                     int32
	ud                       atomic.Value
	sendCloseChan            chan struct{}
	shutdownWriteOnce        int32
	closeOnce                int32
	beginOnce                int32
	sendOnce                 int32
	doCloseOnce              int32
	encoder                  Encoder
	inboundProcessor         InBoundProcessor
	errorCallback            func(*Socket, error)
	closeCallBack            func(*Socket, error)
	inboundCallBack          func(*Socket, interface{})
	closeReason              error
	sendTimeout              int64
	recvTimeout              int64
	muW                      sync.Mutex
	sendCh                   *sendQueue
	b                        *buffer.Buffer
	sendingSize              int
	outputLimit              OutputBufLimit
	obufSoftLimitReachedTime int64
	rwCounter                int64
	p                        *sendp
}

func (s *Socket) GetUnderConn() net.Conn {
	return s.conn
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
	if atomic.CompareAndSwapInt32(&this.closeOnce, 0, 1) {
		this.setFlag(fwclosed)
		if this.writeCount() == 0 {
			this.sendCh.close()
			this.conn.(interface{ CloseWrite() error }).CloseWrite()
		}
	}
}

func (this *Socket) doclose() {
	if this.IsClosed() && atomic.CompareAndSwapInt32(&this.doCloseOnce, 0, 1) {
		this.sendCh.close()
		if nil != this.b {
			this.b.Free()
		}
		if nil != this.closeCallBack {
			this.closeCallBack(this, this.closeReason)
		}
	}
}

func (this *Socket) incRead() {
	for {
		old := atomic.LoadInt64(&this.rwCounter)
		new := (((old >> 32) + 1) << 32) | (old & 0x00000000FFFFFFFF)
		if atomic.CompareAndSwapInt64(&this.rwCounter, old, new) {
			break
		}
	}
}

func (this *Socket) decRead() {
	var newV int64

	for {
		old := atomic.LoadInt64(&this.rwCounter)
		newV = (((old >> 32) - 1) << 32) | (old & 0x00000000FFFFFFFF)
		if atomic.CompareAndSwapInt64(&this.rwCounter, old, newV) {
			break
		}
	}

	if 0 == newV {
		this.doclose()
	}
}

func (this *Socket) incWrite() {
	for {
		old := uint64(atomic.LoadInt64(&this.rwCounter))
		new := ((old & 0x00000000FFFFFFFF) + 1) | (old & 0xFFFFFFFF00000000)
		if atomic.CompareAndSwapInt64(&this.rwCounter, int64(old), int64(new)) {
			break
		}
	}
}

func (this *Socket) decWrite() {
	var newV uint64
	for {
		old := uint64(atomic.LoadInt64(&this.rwCounter))
		newV = ((old & 0x00000000FFFFFFFF) - 1) | (old & 0xFFFFFFFF00000000)
		if atomic.CompareAndSwapInt64(&this.rwCounter, int64(old), int64(newV)) {
			break
		}
	}

	if 0 == newV {
		this.doclose()
	}
}

func (this *Socket) writeCount() int {
	return int(atomic.LoadInt64(&this.rwCounter) & 0x00000000FFFFFFFF)
}

func (this *Socket) recvThreadFunc() {
	defer this.decRead()

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

func (this *Socket) Send(o interface{}) error {
	if nil == o {
		return errors.New("o == nil")
	}

	if this.testFlag(fclosed | fwclosed) {
		return ErrSocketClose
	}

	this.incWrite()

	this.p.runTask(func() {
		this.muW.Lock()
		defer this.decWrite()

		if nil == this.b {
			this.b = buffer.Get()
		}

		if !this.checkOutputLimit(this.sendingSize + this.b.Len()) {
			//超过输出限制，丢包
			//GetSugar().Infof("Drop output msg %d %d %p", this.sendingSize, this.b.Len(), this)
			this.muW.Unlock()
			return
		}

		switch o.(type) {
		case []byte:
			this.b.AppendBytes(o.([]byte))
		default:
			l := this.b.Len()
			if err := this.encoder.EnCode(o, this.b); nil != err {
				GetSugar().Infof("EnCode error:%v", err)
				this.b.SetLen(l)
			}
		}

		if this.b.Len() == 0 {
			this.b.Free()
			this.b = nil
			this.muW.Unlock()
			return
		}

		if this.sendingSize != 0 {
			this.muW.Unlock()
			return
		}

		this.sendingSize = this.b.Len()
		b := this.b
		this.b = nil
		this.muW.Unlock()
		this.incWrite()
		if nil == this.sendCh.push(b) {
			if atomic.CompareAndSwapInt32(&this.sendOnce, 0, 1) {
				go this.sendThreadFunc()
			}
		} else {
			b.Free()
			this.decWrite()
		}
	})

	return nil
}

func (this *Socket) onSendFinish() {
	decWrite := true
	this.muW.Lock()
	defer func() {
		this.muW.Unlock()
		if decWrite {
			this.decWrite()
		}
	}()

	if nil == this.b && this.sendingSize == 0 {
		if this.testFlag(fwclosed) {
			this.sendCh.close()
			this.conn.(interface{ CloseWrite() error }).CloseWrite()
		}
	} else if nil != this.b && this.sendingSize == 0 {
		this.sendingSize = this.b.Len()
		if nil == this.sendCh.push(this.b) {
			this.b = nil
			decWrite = false
		}
	}
}

func (this *Socket) sendThreadFunc() {

	const sendSize int = 65535

	defer close(this.sendCloseChan)

	var err error
	var n int
	oldTimeout := this.getSendTimeout()
	timeout := oldTimeout

	for {
		b, ok := this.sendCh.pop()
		if !ok {
			break
		}

		var buff []byte
		bb := b.Bytes()
		i := 0
		for i < len(bb) {
			oldTimeout = timeout
			timeout = this.getSendTimeout()

			if oldTimeout != timeout && timeout == 0 {
				this.conn.SetWriteDeadline(time.Time{})
			}

			if i+sendSize > len(bb) {
				buff = bb[i:]
			} else {
				buff = bb[i : i+sendSize]
			}

			if timeout > 0 {
				this.conn.SetWriteDeadline(time.Now().Add(timeout))
				n, err = this.conn.Write(buff)
			} else {
				n, err = this.conn.Write(buff)
			}

			i += n

			this.muW.Lock()
			this.sendingSize -= n
			this.muW.Unlock()

			if nil != err {
				if !this.testFlag(fclosed) {
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
						this.decWrite()
						return
					}
				} else {
					b.Free()
					this.decWrite()
					return
				}
			}
		}

		//通告发送完毕
		b.Free()
		this.onSendFinish()

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
			this.incRead()
			go this.recvThreadFunc()
		}
	}

	return
}

func (this *Socket) checkOutputLimit(size int) bool {
	if size > this.outputLimit.OutPutLimitHard {
		return false
	}

	if size > this.outputLimit.OutPutLimitSoft {
		nowUnix := time.Now().Unix()
		if this.obufSoftLimitReachedTime == 0 {
			this.obufSoftLimitReachedTime = nowUnix
		} else {
			elapse := nowUnix - this.obufSoftLimitReachedTime
			if int(elapse) >= this.outputLimit.OutPutLimitSoftSeconds {
				return false
			}
		}
	} else {
		this.obufSoftLimitReachedTime = 0
	}

	return true
}

func (this *Socket) Close(reason error, delay time.Duration) {

	if atomic.CompareAndSwapInt32(&this.closeOnce, 0, 1) {
		runtime.SetFinalizer(this, nil)
		this.setFlag(fclosed)
		this.closeReason = reason

		if delay > 0 {
			if this.writeCount() > 0 {
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
					this.sendCh.close()
				}()
				return
			}
		}

		this.conn.Close()
		this.sendCh.close()
		if atomic.LoadInt64(&this.rwCounter) == 0 {
			this.doclose()
		}
	}
}

func NewSocket(conn net.Conn, outputLimit OutputBufLimit) *Socket {

	switch conn.(type) {
	case *net.TCPConn, *net.UnixConn:
		break
	default:
		return nil
	}

	s := &Socket{
		conn:          conn,
		sendCloseChan: make(chan struct{}),
		sendCh:        &sendQueue{ch: make(chan *buffer.Buffer, 1)},
		outputLimit:   outputLimit,
		p:             &gSendP[int(rand.Int31())%len(gSendP)],
	}

	if s.outputLimit.OutPutLimitHard <= 0 {
		s.outputLimit.OutPutLimitHard = DefaultOutPutLimitHard
	}

	if s.outputLimit.OutPutLimitSoft <= 0 {
		s.outputLimit.OutPutLimitSoft = DefaultOutPutLimitSoft
	}

	if s.outputLimit.OutPutLimitSoftSeconds <= 0 {
		s.outputLimit.OutPutLimitSoftSeconds = DefaultOutPutLimitSoftSeconds
	}

	runtime.SetFinalizer(s, func(s *Socket) {
		s.Close(errors.New("gc"), 0)
	})

	return s
}
