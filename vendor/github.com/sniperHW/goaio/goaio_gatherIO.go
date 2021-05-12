// +build gatherIO

package goaio

import (
	"io"
	"sync/atomic"
	"syscall"
	"time"
	"unsafe"
)

const MaxIovecSize = 64

type aioContext struct {
	index      int //buffs索引
	offset     int //[]byte内下标
	transfered int //已经传输的字节数
	deadline   time.Time
	context    interface{}
	buff       [][]byte
	readfull   bool
}

type AIOResult struct {
	Conn          *AIOConn
	Bytestransfer int
	Buff          [][]byte
	Context       interface{}
	Err           error
}

type AIOConn struct {
	aioConnBase
	send_iovec [MaxIovecSize]syscall.Iovec
	recv_iovec [MaxIovecSize]syscall.Iovec
}

func (this *aioContextQueue) packIovec(iovec *[MaxIovecSize]syscall.Iovec) (int, int) {
	if this.empty() {
		return 0, 0
	} else {
		cc := 0
		total := 0
		ctx := &this.queue[this.head]
		for j := ctx.index; j < len(ctx.buff) && cc < len(*iovec); j++ {
			buff := ctx.buff[ctx.index]
			size := len(buff) - ctx.offset
			(*iovec)[cc] = syscall.Iovec{&buff[ctx.offset], uint64(size)}
			total += size
			cc++
		}
		return cc, total
	}
}

func (this *AIOConn) Send(context interface{}, buffs ...[]byte) error {

	var deadline time.Time

	if 0 != this.sendTimeout {
		deadline = time.Now().Add(this.sendTimeout)
	}

	this.muW.Lock()

	if atomic.LoadInt32(&this.closed) == 1 {
		this.muW.Unlock()
		return ErrConnClosed
	}

	if err := this.w.add(aioContext{
		buff:     buffs,
		context:  context,
		deadline: deadline,
	}); nil != err {
		this.muW.Unlock()
		if this.connMgr.isClosed() {
			return ErrServiceClosed
		} else {
			return err
		}
	}

	if !this.connMgr.addIO(this) {
		this.muW.Unlock()
		return ErrServiceClosed
	}

	if !deadline.IsZero() && nil == this.wtimer {
		this.wtimer = time.AfterFunc(this.sendTimeout, this.onWriteTimeout)
	}

	if this.writeable && !this.doingW {
		this.doingW = true
		this.muW.Unlock()
		if !this.service.deliverTask(&task{conn: this, tt: int64(EV_WRITE)}) {
			return ErrServiceClosed
		}
	} else {
		this.muW.Unlock()
	}

	return nil

}

func (this *AIOConn) recv(context interface{}, readfull bool, buffs ...[]byte) error {
	var deadline time.Time

	if 0 != this.recvTimeout {
		deadline = time.Now().Add(this.recvTimeout)
	}

	this.muR.Lock()

	if atomic.LoadInt32(&this.closed) == 1 {
		this.muR.Unlock()
		return ErrConnClosed
	}

	if err := this.r.add(aioContext{
		buff:     buffs,
		context:  context,
		deadline: deadline,
		readfull: readfull,
	}); nil != err {
		this.muR.Unlock()
		if this.connMgr.isClosed() {
			return ErrServiceClosed
		} else {
			return err
		}
	}

	if !this.connMgr.addIO(this) {
		this.muR.Unlock()
		return ErrServiceClosed
	}

	if !deadline.IsZero() && nil == this.rtimer {
		this.rtimer = time.AfterFunc(this.recvTimeout, this.onReadTimeout)
	}

	if this.readable && !this.doingR {
		this.doingR = true
		this.muR.Unlock()
		if !this.service.deliverTask(&task{conn: this, tt: int64(EV_READ)}) {
			return ErrServiceClosed
		}
	} else {
		this.muR.Unlock()
	}

	return nil
}

func (this *AIOConn) Recv(context interface{}, buffs ...[]byte) error {
	return this.recv(context, false, buffs...)
}

func (this *AIOConn) RecvFull(context interface{}, buffs ...[]byte) error {
	return this.recv(context, true, buffs...)
}

func (this *AIOConn) doRead() {

	this.muR.Lock()

	for atomic.LoadInt32(&this.closed) == 0 && this.readable && !this.r.empty() {

		c := this.r.front()
		ver := this.readableVer

		this.muR.Unlock()

		userShareBuffer := false
		var sharebuff []byte
		var cc int
		var total int

		cc, total = this.r.packIovec(&this.recv_iovec)
		if 0 == total {
			if nil != this.sharebuff {
				sharebuff = this.sharebuff.Acquire()
			}

			if len(sharebuff) > 0 {
				this.recv_iovec[0] = syscall.Iovec{&sharebuff[0], uint64(len(sharebuff))}
				userShareBuffer = true
			} else {
				c.buff = append(c.buff, make([]byte, DefaultRecvBuffSize))
				this.recv_iovec[0] = syscall.Iovec{&c.buff[0][0], uint64(DefaultRecvBuffSize)}
			}

			cc = 1
		}

		var (
			r uintptr
			e syscall.Errno
		)

		r, _, e = syscall.RawSyscall(syscall.SYS_READV, uintptr(this.fd), uintptr(unsafe.Pointer(&this.recv_iovec[0])), uintptr(cc))
		size := int(r)

		this.muR.Lock()

		if e == syscall.EINTR {
			continue
		} else if size == 0 || (e != 0 && e != syscall.EAGAIN) {

			var err error
			if size == 0 {
				err = io.EOF
			} else {
				err = e
			}

			if userShareBuffer {
				this.sharebuff.Release(sharebuff)
			}

			for !this.r.empty() {
				c := this.r.front()
				this.service.postCompleteStatus(this, c.buff, c.transfered, err, c.context)
				this.r.popFront()
			}

		} else if e == syscall.EAGAIN {
			if ver == this.readableVer {
				this.readable = false
			}

			if userShareBuffer {
				this.sharebuff.Release(sharebuff)
			}

		} else {

			if userShareBuffer {
				c.buff = append(c.buff, sharebuff)
			}

			if !userShareBuffer && c.readfull {

				remain := size
				for remain > 0 {
					s := len(c.buff[c.index][c.offset:])
					if remain >= s {
						remain -= s
						c.transfered += s
						c.index++
						c.offset = 0
					} else {
						c.offset += remain
						c.transfered += remain
						remain = 0
					}
				}

				if c.index >= len(c.buff) {
					this.service.postCompleteStatus(this, c.buff, c.transfered, nil, c.context)
					this.r.popFront()
				}

			} else {
				this.service.postCompleteStatus(this, c.buff, size, nil, c.context)
				this.r.popFront()
			}
		}
	}

	if atomic.LoadInt32(&this.closed) == 1 {
		for !this.r.empty() {
			c := this.r.front()
			this.service.postCompleteStatus(this, c.buff, c.transfered, this.reason, c.context)
			this.r.popFront()
		}
	} else if nil != this.dorTimer && this.rtimer == this.dorTimer {
		this.dorTimer = nil
		this.processReadTimeout()
	}

	this.doingR = false
	this.muR.Unlock()
}

func (this *AIOConn) doWrite() {

	this.muW.Lock()

	for atomic.LoadInt32(&this.closed) == 0 && this.writeable && !this.w.empty() {

		cc, total := this.w.packIovec(&this.send_iovec)
		if 0 == total {
			c := this.w.front()
			this.service.postCompleteStatus(this, c.buff, c.transfered, ErrEmptyBuff, c.context)
			this.w.popFront()
			continue
		}

		ver := this.writeableVer
		this.muW.Unlock()

		var (
			r uintptr
			e syscall.Errno
		)

		r, _, e = syscall.RawSyscall(syscall.SYS_WRITEV, uintptr(this.fd), uintptr(unsafe.Pointer(&this.send_iovec[0])), uintptr(cc))
		size := int(r)

		this.muW.Lock()

		if e == syscall.EINTR {
			continue
		} else if size == 0 || (e != 0 && e != syscall.EAGAIN) {

			var err error
			if size == 0 {
				err = io.ErrUnexpectedEOF
			} else {
				err = e
			}

			for !this.w.empty() {
				c := this.w.front()
				this.service.postCompleteStatus(this, c.buff, c.transfered, err, c.context)
				this.w.popFront()
			}
		} else if e == syscall.EAGAIN {
			if ver == this.writeableVer {
				this.writeable = false
			}
		} else {
			remain := size
			c := this.w.front()
			for remain > 0 {
				s := len(c.buff[c.index][c.offset:])
				if remain >= s {
					remain -= s
					c.transfered += s
					c.index++
					c.offset = 0
				} else {
					c.offset += remain
					c.transfered += remain
					remain = 0
				}
			}

			if c.index >= len(c.buff) {
				this.service.postCompleteStatus(this, c.buff, c.transfered, nil, c.context)
				this.w.popFront()
			}
		}
	}

	if atomic.LoadInt32(&this.closed) == 1 {
		for !this.w.empty() {
			c := this.w.front()
			this.service.postCompleteStatus(this, c.buff, c.transfered, this.reason, c.context)
			this.w.popFront()
		}
	} else if nil != this.dowTimer && this.wtimer == this.dowTimer {
		this.dowTimer = nil
		this.processWriteTimeout()
	}

	this.doingW = false
	this.muW.Unlock()
}

func (this *AIOService) postCompleteStatus(c *AIOConn, buff [][]byte, bytestransfer int, err error, context interface{}) {
	c.connMgr.subIO(c)
	select {
	case <-this.die:
		return
	default:
		this.completeQueue <- AIOResult{
			Conn:          c,
			Context:       context,
			Err:           err,
			Buff:          buff,
			Bytestransfer: bytestransfer,
		}
	}
}
