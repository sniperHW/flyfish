package goaio

import (
	"errors"
	"io"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

var (
	ErrRecvTimeout   = errors.New("RecvTimeout")
	ErrSendTimeout   = errors.New("SendTimetout")
	ErrConnClosed    = errors.New("conn closed")
	ErrServiceClosed = errors.New("service closed")
	ErrUnsupportConn = errors.New("net.Conn does implement net.RawConn")
	ErrWatchFailed   = errors.New("watch failed")
	ErrActiveClose   = errors.New("active close")
	ErrCloseNone     = errors.New("close no reason")
	ErrCloseGC       = errors.New("close by gc")
	ErrEmptyBuff     = errors.New("buffs is empty")
)

const (
	EV_READ  = int(1 << 1)
	EV_WRITE = int(1 << 2)
	EV_ERROR = int(1 << 3)
	//这两值定义为常量性能更好，原因尚不明
	CompleteQueueSize     = 65535 * 2
	TaskQueueSize         = 65535 * 2
	ConnMgrhashMask   int = 16
	ConnMgrhashSize   int = 1 << ConnMgrhashMask
)

var (
	DefaultWorkerCount  = 1
	DefaultRecvBuffSize = 4096
)

type ShareBuffer interface {
	Acquire() []byte
	Release([]byte)
}

type AIOConnOption struct {
	ShareBuff ShareBuffer
}

func pushContext(head *aioContext, n *aioContext) {
	if head != n {
		tail := head.pprev

		n.nnext = tail.nnext
		n.pprev = tail

		tail.nnext = n
		head.pprev = n
	}
}

func popContext(head *aioContext) *aioContext {
	if head.nnext == head {
		return nil
	} else {
		first := head.nnext
		removeContext(first)
		return first
	}
}

func removeContext(n *aioContext) {
	if nil != n.nnext && nil != n.pprev && n.nnext != n && n.pprev != n {
		next := n.nnext
		prev := n.pprev
		prev.nnext = next
		next.pprev = prev
		n.nnext = nil
		n.pprev = nil
	}
}

type AIOConn struct {
	fd             int
	rawconn        net.Conn
	muR            sync.Mutex
	muW            sync.Mutex
	readableVer    int
	writeableVer   int
	w              *aioContext
	r              *aioContext
	firstWDeadline time.Time
	firstRDeadline time.Time
	rtimer         *time.Timer
	dorTimer       *time.Timer
	wtimer         *time.Timer
	dowTimer       *time.Timer
	service        *AIOService
	closed         int32
	closeOnce      sync.Once
	sendTimeout    time.Duration
	recvTimeout    time.Duration
	reason         error
	sharebuff      ShareBuffer
	pprev          *AIOConn
	nnext          *AIOConn
	ioCount        int
	connMgr        *connMgr
	readable       bool
	writeable      bool
	doingW         bool
	doingR         bool
	taskR          func()
	taskW          func()
}

type AIOService struct {
	sync.Mutex
	completeQueue chan AIOResult
	taskPool      *taskPool
	poller        pollerI
	die           chan struct{}
	closeOnce     sync.Once
	connMgr       [ConnMgrhashSize]connMgr
}

type connMgr struct {
	sync.Mutex
	head   AIOConn
	closed bool
}

var defalutService *AIOService

var createOnce sync.Once

type aioContext struct {
	nnext    *aioContext
	pprev    *aioContext
	offset   int
	deadline time.Time
	buff     []byte
	context  interface{}
	readfull bool
}

var aioContextPool *sync.Pool = &sync.Pool{
	New: func() interface{} {
		return &aioContext{}
	},
}

func getAioContext() *aioContext {
	return aioContextPool.Get().(*aioContext)
}

func putAioContext(c *aioContext) {
	c.offset = 0
	c.deadline = time.Time{}
	c.buff = nil
	c.context = nil
	c.readfull = false
	aioContextPool.Put(c)
}

type AIOResult struct {
	Conn          *AIOConn
	Bytestransfer int
	Buff          []byte
	Context       interface{}
	Err           error
}

func (this *AIOConn) send(context interface{}, buff []byte, timeout time.Duration, dosend bool) error {

	if atomic.LoadInt32(&this.closed) == 1 {
		return ErrConnClosed
	}

	if !this.connMgr.addIO(this) {
		return ErrServiceClosed
	}

	c := getAioContext()
	c.buff = buff
	c.context = context

	doW := false

	this.muW.Lock()

	if timeout > 0 {
		c.deadline = time.Now().Add(timeout)
		if this.firstWDeadline.IsZero() || c.deadline.Before(this.firstWDeadline) {
			this.firstWDeadline = c.deadline
			if nil == this.wtimer || this.wtimer.Stop() {
				this.wtimer = time.AfterFunc(this.firstWDeadline.Sub(time.Now()), this.onWriteTimeout)
			}
		}
	}

	pushContext(this.w, c)

	if this.writeable && !this.doingW {
		this.doingW = true
		doW = true
	}

	this.muW.Unlock()

	if doW {
		if dosend {
			this.doWrite()
		} else {
			this.service.deliverTask(this.taskW)
		}
	}

	return nil
}

func (this *AIOConn) recv(context interface{}, readfull bool, buff []byte, timeout time.Duration, doread bool) error {

	if atomic.LoadInt32(&this.closed) == 1 {
		return ErrConnClosed
	}

	if !this.connMgr.addIO(this) {
		return ErrServiceClosed
	}

	c := getAioContext()
	c.buff = buff
	c.context = context
	c.readfull = readfull

	doR := false

	this.muR.Lock()
	if timeout > 0 {
		c.deadline = time.Now().Add(timeout)
		if this.firstRDeadline.IsZero() || c.deadline.Before(this.firstRDeadline) {
			this.firstRDeadline = c.deadline
			if nil == this.rtimer || this.rtimer.Stop() {
				this.rtimer = time.AfterFunc(this.firstRDeadline.Sub(time.Now()), this.onReadTimeout)
			}
		}
	}

	pushContext(this.r, c)

	if this.readable && !this.doingR {
		doread = doread && !(len(buff) == 0 && nil != this.sharebuff)
		this.doingR = true
		doR = true
	}

	this.muR.Unlock()
	if doR {
		if doread {
			this.doRead()
		} else {
			this.service.deliverTask(this.taskR)
		}
	}
	return nil
}

func (this *AIOConn) Send(context interface{}, buff []byte, timeout time.Duration) error {
	return this.send(context, buff, timeout, false)
}

func (this *AIOConn) Recv(context interface{}, buff []byte, timeout time.Duration) error {
	return this.recv(context, false, buff, timeout, false)
}

func (this *AIOConn) RecvFull(context interface{}, buff []byte, timeout time.Duration) error {
	return this.recv(context, true, buff, timeout, false)
}

/*
 * 以下3个接口在可能的情况下会立即在本地执行io任务
 *
 * 禁止在completeRoutine中使用以下3个接口。因为接口可能触发向completeQueue投递的操作。
 * 如果此时completeQueue满将导致死锁（当前线程阻塞在postCompleteStatus上,因此无法调用GetCompleteStatus去解除阻塞）。
 *
 */

func (this *AIOConn) Send1(context interface{}, buff []byte, timeout time.Duration) error {
	return this.send(context, buff, timeout, true)
}

func (this *AIOConn) Recv1(context interface{}, buff []byte, timeout time.Duration) error {
	return this.recv(context, false, buff, timeout, true)
}

func (this *AIOConn) RecvFull1(context interface{}, buff []byte, timeout time.Duration) error {
	return this.recv(context, true, buff, timeout, true)
}

func (this *AIOConn) doRead() {

	this.muR.Lock()

	for atomic.LoadInt32(&this.closed) == 0 && this.readable && this.r.nnext != this.r {

		c := this.r.nnext
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

			for this.r.nnext != this.r {
				n := popContext(this.r)
				this.service.postCompleteStatus(this, n.buff, 0, err, n.context)
				putAioContext(n)
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

			removeContext(c)
			putAioContext(c)
		}
	}

	if atomic.LoadInt32(&this.closed) == 1 {
		for this.r.nnext != this.r {
			n := popContext(this.r)
			this.service.postCompleteStatus(this, n.buff, n.offset, this.reason, n.context)
			putAioContext(n)
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

	for atomic.LoadInt32(&this.closed) == 0 && this.writeable && this.w.nnext != this.w {

		c := this.w.nnext

		if 0 == len(c.buff) {
			this.service.postCompleteStatus(this, c.buff, 0, ErrEmptyBuff, c.context)
			removeContext(c)
			putAioContext(c)
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

			for this.w.nnext != this.w {
				n := popContext(this.w)
				this.service.postCompleteStatus(this, n.buff, n.offset, err, n.context)
				putAioContext(n)
			}
		} else if err == syscall.EAGAIN {
			if ver == this.writeableVer {
				this.writeable = false
			}
		} else {
			c.offset += size
			if c.offset >= len(c.buff) {
				this.service.postCompleteStatus(this, c.buff, c.offset, nil, c.context)
				removeContext(c)
				putAioContext(c)
			}
		}
	}

	if atomic.LoadInt32(&this.closed) == 1 {
		for this.w.nnext != this.w {
			n := popContext(this.w)
			this.service.postCompleteStatus(this, n.buff, n.offset, this.reason, n.context)
			putAioContext(n)
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
		if atomic.LoadInt32(&c.closed) == 0 {
			this.completeQueue <- AIOResult{
				Conn:          c,
				Context:       context,
				Err:           err,
				Buff:          buff,
				Bytestransfer: bytestransfer,
			}
		}
	}
}

func (this *AIOConn) Close(reason error) {
	this.closeOnce.Do(func() {
		runtime.SetFinalizer(this, nil)

		if nil == reason {
			reason = ErrCloseNone
		}
		this.reason = reason

		atomic.StoreInt32(&this.closed, 1)

		this.muR.Lock()
		if nil != this.rtimer {
			this.rtimer.Stop()
			this.rtimer = nil
		}

		if !this.doingR {
			this.doingR = true
			this.service.deliverTask(this.taskR)
		}
		this.muR.Unlock()

		this.muW.Lock()
		if nil != this.wtimer {
			this.wtimer.Stop()
			this.wtimer = nil
		}

		if !this.doingW {
			this.doingW = true
			this.service.deliverTask(this.taskW)
		}
		this.muW.Unlock()

		this.service.unwatch(this)

		this.rawconn.Close()

	})
}

func (this *AIOConn) processReadTimeout() {
	now := time.Now()
	if now.After(this.firstRDeadline) {
		this.firstRDeadline = time.Time{}
		n := this.r.nnext
		for n != this.r {
			if now.After(n.deadline) {
				nn := n.nnext
				removeContext(n)
				this.service.postCompleteStatus(this, n.buff, n.offset, ErrRecvTimeout, n.context)
				putAioContext(n)
				n = nn
			} else {
				if !n.deadline.IsZero() && (this.firstRDeadline.IsZero() || n.deadline.Before(this.firstRDeadline)) {
					this.firstRDeadline = n.deadline
				}
				n = n.nnext
			}
		}
	}

	if !this.firstRDeadline.IsZero() {
		this.rtimer = time.AfterFunc(this.firstRDeadline.Sub(time.Now()), this.onReadTimeout)
	} else {
		this.rtimer = nil
	}

}

func (this *AIOConn) processWriteTimeout() {
	now := time.Now()
	if now.After(this.firstWDeadline) {
		this.firstWDeadline = time.Time{}
		n := this.w.nnext
		for n != this.w {
			if now.After(n.deadline) {
				nn := n.nnext
				removeContext(n)
				this.service.postCompleteStatus(this, n.buff, n.offset, ErrSendTimeout, n.context)
				putAioContext(n)
				n = nn
			} else {
				if !n.deadline.IsZero() && (this.firstWDeadline.IsZero() || n.deadline.Before(this.firstWDeadline)) {
					this.firstWDeadline = n.deadline
				}
				n = n.nnext
			}
		}
	}

	if !this.firstWDeadline.IsZero() {
		this.wtimer = time.AfterFunc(this.firstWDeadline.Sub(time.Now()), this.onWriteTimeout)
	} else {
		this.wtimer = nil
	}

}

func (this *AIOConn) onReadTimeout() {
	this.muR.Lock()
	defer this.muR.Unlock()
	if atomic.LoadInt32(&this.closed) == 0 {
		this.dorTimer = this.rtimer
		if nil != this.dorTimer && !this.doingR {
			this.doingR = true
			this.service.deliverTask(this.taskR)
		}
	}
}

func (this *AIOConn) onWriteTimeout() {
	this.muW.Lock()
	defer this.muW.Unlock()
	if atomic.LoadInt32(&this.closed) == 0 {
		this.dowTimer = this.wtimer
		if nil != this.dowTimer && !this.doingW {
			this.doingW = true
			this.service.deliverTask(this.taskW)
		}
	}
}

func (this *AIOConn) onActive(ev int) {

	if atomic.LoadInt32(&this.closed) == 1 {
		return
	}

	if ev&EV_READ != 0 || ev&EV_ERROR != 0 {
		this.muR.Lock()
		this.readable = true
		this.readableVer++
		if this.r.nnext != this.r && !this.doingR {
			this.doingR = true
			this.muR.Unlock()
			this.service.deliverTask(this.taskR)
		} else {
			this.muR.Unlock()
		}
	}

	if ev&EV_WRITE != 0 || ev&EV_ERROR != 0 {
		this.muW.Lock()
		this.writeable = true
		this.writeableVer++
		if this.w.nnext != this.w && !this.doingW {
			this.doingW = true
			this.muW.Unlock()
			this.service.deliverTask(this.taskW)
		} else {
			this.muW.Unlock()
		}
	}
}

/*
 *  AIOConn投递io请求时持有AIOConn的引用，避免AIOConn被GC
 */
func (this *connMgr) addIO(c *AIOConn) bool {
	this.Lock()
	defer this.Unlock()
	if !this.closed {
		c.ioCount++
		if c.ioCount == 1 {
			next := this.head.nnext
			c.nnext = next
			c.pprev = &this.head
			this.head.nnext = c
			if next != &this.head {
				next.pprev = c
			}
		}
		return true
	} else {
		return false
	}
}

func (this *connMgr) subIO(c *AIOConn) {
	this.Lock()
	defer this.Unlock()
	if !this.closed {
		c.ioCount--
		if 0 == c.ioCount {
			prev := c.pprev
			next := c.nnext
			prev.nnext = next
			next.pprev = prev
			c.pprev = nil
			c.nnext = nil
		}
	}
}

func (this *connMgr) close() {
	this.Lock()
	defer this.Unlock()
	this.closed = true
	this.head.nnext = nil
}

func NewAIOService(worker int) (s *AIOService) {
	if poller, err := openPoller(); nil == err {
		s = &AIOService{
			completeQueue: make(chan AIOResult, CompleteQueueSize),
			poller:        poller,
			taskPool:      newTaskPool(worker),
			die:           make(chan struct{}),
		}

		runtime.SetFinalizer(s, func(s *AIOService) {
			s.Close()
		})

		for k, _ := range s.connMgr {
			s.connMgr[k].head.nnext = &s.connMgr[k].head
		}

		go poller.wait(s.die)
	}
	return
}

func (this *AIOService) unwatch(c *AIOConn) {
	this.poller.unwatch(c)
}

func (this *AIOService) CreateAIOConn(conn net.Conn, option AIOConnOption) (*AIOConn, error) {

	this.Lock()
	defer this.Unlock()

	select {
	case <-this.die:
		return nil, ErrServiceClosed
	default:

		c, ok := conn.(interface {
			SyscallConn() (syscall.RawConn, error)
		})

		if !ok {
			return nil, ErrUnsupportConn
		}

		rawconn, err := c.SyscallConn()
		if err != nil {
			return nil, err
		}

		var fd int

		if err := rawconn.Control(func(s uintptr) {
			fd = int(s)
		}); err != nil {
			return nil, err
		}

		syscall.SetNonblock(fd, true)

		cc := &AIOConn{
			fd:        fd,
			rawconn:   conn,
			service:   this,
			sharebuff: option.ShareBuff,
			connMgr:   &this.connMgr[fd>>ConnMgrhashSize],
			w:         &aioContext{},
			r:         &aioContext{},
		}

		cc.taskR = func() {
			cc.doRead()
		}

		cc.taskW = func() {
			cc.doWrite()
		}

		cc.w.nnext = cc.w
		cc.w.pprev = cc.w

		cc.r.nnext = cc.r
		cc.r.pprev = cc.r

		ok = <-this.poller.watch(cc)
		if ok {
			runtime.SetFinalizer(cc, func(cc *AIOConn) {
				cc.Close(ErrCloseGC)
			})
			return cc, nil
		} else {
			return nil, ErrWatchFailed
		}
	}
}

func (this *AIOService) deliverTask(task func()) {
	select {
	case <-this.die:
	default:
		this.taskPool.addTask(task)
	}
}

func (this *AIOService) GetCompleteStatus() (AIOResult, bool) {
	select {
	case <-this.die:
		return AIOResult{}, false
	case r := <-this.completeQueue:
		return r, true
	}
}

func (this *AIOService) Close() {
	this.closeOnce.Do(func() {
		runtime.SetFinalizer(this, nil)
		this.Lock()
		defer this.Unlock()

		close(this.die)

		this.taskPool.close()

		this.poller.close()

		for _, v := range this.connMgr {
			v.close()
		}
	})
}

func CreateAIOConn(conn net.Conn, option AIOConnOption) (*AIOConn, error) {
	createOnce.Do(func() {
		defalutService = NewAIOService(DefaultWorkerCount)
	})
	return defalutService.CreateAIOConn(conn, option)
}

func GetCompleteStatus() (AIOResult, bool) {
	createOnce.Do(func() {
		defalutService = NewAIOService(DefaultWorkerCount)
	})
	return defalutService.GetCompleteStatus()
}
