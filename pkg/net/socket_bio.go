// +build !aio

package net

import (
	"errors"
	"github.com/sniperHW/flyfish/pkg/buffer"
	"github.com/sniperHW/flyfish/pkg/queue"
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

type Socket struct {
	socketBase
	sendQue     *queue.ArrayQueue
	sendTimeout int64
	recvTimeout int64
}

func (this *Socket) ShutdownWrite() {
	if this.sendQue.Close() {
		this.sendOnce.Do(func() {
			this.addIO()
			go this.sendThreadFunc()
		})
	}
}

func (this *Socket) SetRecvTimeout(timeout time.Duration) *Socket {
	atomic.StoreInt64(&this.recvTimeout, int64(timeout))
	return this
}

func (this *Socket) SetSendTimeout(timeout time.Duration) *Socket {
	atomic.StoreInt64(&this.sendTimeout, int64(timeout))
	return this
}

func (this *Socket) SetSendQueueSize(size int) *Socket {
	this.sendQue.SetCap(size)
	return this
}

func (this *Socket) getRecvTimeout() time.Duration {
	return time.Duration(atomic.LoadInt64(&this.recvTimeout))
}

func (this *Socket) getSendTimeout() time.Duration {
	return time.Duration(atomic.LoadInt64(&this.sendTimeout))
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
	defer this.ioDone()
	defer close(this.sendCloseChan)

	var err error

	localList := make([]interface{}, 0, 32)

	closed := false

	var n int

	oldTimeout := this.getSendTimeout()
	timeout := oldTimeout

	for {

		localList, closed = this.sendQue.Pop(localList)
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

	this.beginOnce.Do(func() {
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
	})

	return
}

func (this *Socket) Send(o interface{}) (err error) {
	if nil == o {
		return errors.New("o == nil")
	}
	err = this.sendQue.Append(o)
	if err == queue.ErrQueueClosed {
		err = ErrSocketClose
	} else if err == queue.ErrQueueFull {
		err = ErrSendQueFull
	} else {
		this.sendOnce.Do(func() {
			this.addIO()
			go this.sendThreadFunc()
		})
	}
	return
}

func (this *Socket) ioDone() {
	if 0 == atomic.AddInt32(&this.ioCount, -1) && this.testFlag(fdoclose) {
		if nil != this.closeCallBack {
			this.closeCallBack(this, this.closeReason)
		}
	}
}

func (this *Socket) Close(reason error, delay time.Duration) {

	this.closeOnce.Do(func() {
		runtime.SetFinalizer(this, nil)

		this.setFlag(fclosed)

		wclosed := this.sendQue.Closed()

		this.sendQue.Close()

		if !wclosed && delay > 0 {
			func() {
				this.sendOnce.Do(func() {
					this.addIO()
					go this.sendThreadFunc()
				})
			}()
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
			if nil != this.closeCallBack {
				this.closeCallBack(this, reason)
			}
		}

	})

}

func NewSocket(conn net.Conn) *Socket {

	switch conn.(type) {
	case *net.TCPConn, *net.UnixConn:
		break
	default:
		return nil
	}

	s := &Socket{
		socketBase: socketBase{
			conn:          conn,
			sendCloseChan: make(chan struct{}),
		},
		sendQue: queue.NewArrayQueue(128),
	}

	runtime.SetFinalizer(s, func(s *Socket) {
		s.Close(errors.New("gc"), 0)
	})

	return s
}

func CreateSocket(conn net.Conn) *Socket {
	return NewSocket(conn)
}
