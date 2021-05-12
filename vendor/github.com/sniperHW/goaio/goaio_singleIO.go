// +build !gatherIO

package goaio

import (
	"io"
	"sync/atomic"
	"syscall"
	"time"
)

type AIOConn struct {
	aioConnBase
}

type aioContext struct {
	offset   int
	deadline time.Time
	buff     []byte
	context  interface{}
	readfull bool
}

type AIOResult struct {
	Conn          *AIOConn
	Bytestransfer int
	Buff          []byte
	Context       interface{}
	Err           error
}

func (this *AIOConn) Send(context interface{}, buff []byte) error {

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
		buff:     buff,
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

func (this *AIOConn) recv(context interface{}, readfull bool, buff []byte) error {
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
		buff:     buff,
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

func (this *AIOConn) Recv(context interface{}, buff []byte) error {
	return this.recv(context, false, buff)
}

func (this *AIOConn) RecvFull(context interface{}, buff []byte) error {
	return this.recv(context, true, buff)
}

func (this *AIOConn) doRead() {

	this.muR.Lock()

	for atomic.LoadInt32(&this.closed) == 0 && this.readable && !this.r.empty() {

		c := this.r.front()
		ver := this.readableVer
		this.muR.Unlock()
		var buff []byte
		userShareBuffer := false
		if len(c.buff) == 0 {
			if nil != this.sharebuff {
				buff = this.sharebuff.Acquire()
			}

			if len(buff) > 0 {
				userShareBuffer = true
			} else {
				userShareBuffer = false
				buff = make([]byte, DefaultRecvBuffSize)
				c.buff = buff
			}
		} else {
			buff = c.buff
		}

		size, err := rawRead(this.fd, buff[c.offset:])

		this.muR.Lock()
		if err == syscall.EINTR {
			continue
		} else if size == 0 || (err != nil && err != syscall.EAGAIN) {
			if size == 0 {
				err = io.EOF
			}

			if userShareBuffer {
				this.sharebuff.Release(buff)
				buff = nil
			}

			for !this.r.empty() {
				c := this.r.front()
				this.service.postCompleteStatus(this, c.buff, 0, err, c.context)
				this.r.popFront()
			}

		} else if err == syscall.EAGAIN {
			if ver == this.readableVer {
				this.readable = false
			}

			if userShareBuffer {
				this.sharebuff.Release(buff)
			}

		} else {

			c.offset += size

			if !userShareBuffer && c.readfull && c.offset < len(c.buff) {
				continue
			}

			this.service.postCompleteStatus(this, buff, size, nil, c.context)
			this.r.popFront()
		}
	}

	if atomic.LoadInt32(&this.closed) == 1 {
		for !this.r.empty() {
			c := this.r.front()
			this.service.postCompleteStatus(this, c.buff, c.offset, this.reason, c.context)
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

		c := this.w.front()

		if 0 == len(c.buff) {
			this.service.postCompleteStatus(this, c.buff, 0, ErrEmptyBuff, c.context)
			this.w.popFront()
			continue
		}

		ver := this.writeableVer
		this.muW.Unlock()

		size, err := rawWrite(this.fd, c.buff[c.offset:])

		this.muW.Lock()

		if err == syscall.EINTR {
			continue
		} else if size == 0 || (err != nil && err != syscall.EAGAIN) {
			if size == 0 {
				err = io.ErrUnexpectedEOF
			}

			for !this.w.empty() {
				c := this.w.front()
				this.service.postCompleteStatus(this, c.buff, c.offset, err, c.context)
				this.w.popFront()
			}
		} else if err == syscall.EAGAIN {
			if ver == this.writeableVer {
				this.writeable = false
			}
		} else {
			c.offset += size
			if c.offset >= len(c.buff) {
				this.service.postCompleteStatus(this, c.buff, c.offset, nil, c.context)
				this.w.popFront()
			}
		}
	}

	if atomic.LoadInt32(&this.closed) == 1 {
		for !this.w.empty() {
			c := this.w.front()
			this.service.postCompleteStatus(this, c.buff, c.offset, this.reason, c.context)
			this.w.popFront()
		}
	} else if nil != this.dowTimer && this.wtimer == this.dowTimer {
		this.dowTimer = nil
		this.processWriteTimeout()
	}

	this.doingW = false
	this.muW.Unlock()
}

func (this *AIOService) postCompleteStatus(c *AIOConn, buff []byte, bytestransfer int, err error, context interface{}) {
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
