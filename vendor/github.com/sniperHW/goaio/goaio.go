package goaio

import (
	"errors"
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
	ErrBusy          = errors.New("busy")
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

type task struct {
	tt   int64
	conn *AIOConn
}

type AIOConnOption struct {
	SendqueSize int
	RecvqueSize int
	ShareBuff   ShareBuffer
	UserData    interface{}
}

type aioContextQueue struct {
	head  int
	tail  int
	queue []aioContext
}

type aioConnBase struct {
	fd           int
	rawconn      net.Conn
	muR          sync.Mutex
	muW          sync.Mutex
	readableVer  int
	writeableVer int
	w            *aioContextQueue
	r            *aioContextQueue
	rtimer       *time.Timer
	dorTimer     *time.Timer
	wtimer       *time.Timer
	dowTimer     *time.Timer
	service      *AIOService
	closed       int32
	closeOnce    sync.Once
	sendTimeout  time.Duration
	recvTimeout  time.Duration
	reason       error
	sharebuff    ShareBuffer
	userData     interface{}
	pprev        *AIOConn
	nnext        *AIOConn
	ioCount      int
	connMgr      *connMgr
	tq           chan task
	readable     bool
	writeable    bool
	doingW       bool
	doingR       bool
}

type AIOService struct {
	sync.Mutex
	completeQueue chan AIOResult
	tq            chan task
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

func newAioContextQueue(max int) *aioContextQueue {
	if max <= 0 {
		max = 32
	}
	return &aioContextQueue{
		queue: make([]aioContext, max+1, max+1),
	}
}

func (this *aioContextQueue) empty() bool {
	return this.head == this.tail
}

func (this *aioContextQueue) add(c aioContext) error {
	if next := (this.tail + 1) % len(this.queue); next == this.head {
		return ErrBusy
	} else {
		this.queue[this.tail] = c
		this.tail = next
		return nil
	}
}

func (this *aioContextQueue) front() *aioContext {
	return &this.queue[this.head]
}

func (this *aioContextQueue) popFront() {
	if this.head != this.tail {
		this.queue[this.head] = aioContext{}
		this.head = (this.head + 1) % len(this.queue)
	}
}

func (this *aioContextQueue) setDeadline(deadline time.Time) bool {
	if this.empty() {
		return false
	} else {
		i := this.head
		for i != this.tail {
			this.queue[i].deadline = deadline
			i = (i + 1) % len(this.queue)
		}
		return true
	}
}

func (this *AIOConn) GetUserData() interface{} {
	return this.userData
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
			this.service.deliverTask(&task{conn: this, tt: int64(EV_READ)})
		}
		this.muR.Unlock()

		this.muW.Lock()
		if nil != this.wtimer {
			this.wtimer.Stop()
			this.wtimer = nil
		}

		if !this.doingW {
			this.doingW = true
			this.service.deliverTask(&task{conn: this, tt: int64(EV_WRITE)})
		}
		this.muW.Unlock()

		this.service.unwatch(this)

		this.rawconn.Close()

	})
}

func (this *AIOConn) processReadTimeout() {
	now := time.Now()
	if this.recvTimeout > 0 {
		for !this.r.empty() {
			f := this.r.front()
			if now.After(f.deadline) {
				this.service.postCompleteStatus(this, f.buff, f.offset, ErrRecvTimeout, f.context)
				this.r.popFront()
			} else {
				break
			}
		}

		if !this.r.empty() {
			this.rtimer = time.AfterFunc(now.Sub(this.r.front().deadline), this.onReadTimeout)
			return
		}
	}

	this.rtimer = nil
}

func (this *AIOConn) processWriteTimeout() {
	now := time.Now()

	if this.sendTimeout > 0 {
		for !this.w.empty() {
			f := this.w.front()
			if now.After(f.deadline) {
				this.service.postCompleteStatus(this, f.buff, f.offset, ErrSendTimeout, f.context)
				this.w.popFront()
			} else {
				break
			}
		}

		if !this.w.empty() {
			this.wtimer = time.AfterFunc(now.Sub(this.w.front().deadline), this.onWriteTimeout)
			return
		}
	}

	this.wtimer = nil

}

func (this *AIOConn) onReadTimeout() {
	this.muR.Lock()
	defer this.muR.Unlock()
	if atomic.LoadInt32(&this.closed) == 0 {
		this.dorTimer = this.rtimer
		if nil != this.dorTimer && !this.doingR {
			this.doingR = true
			this.service.deliverTask(&task{conn: this, tt: int64(EV_READ)})
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
			this.service.deliverTask(&task{conn: this, tt: int64(EV_WRITE)})
		}
	}
}

func (this *AIOConn) SetRecvTimeout(timeout time.Duration) {
	this.muR.Lock()
	defer this.muR.Unlock()
	if atomic.LoadInt32(&this.closed) == 0 {
		if nil != this.rtimer {
			this.rtimer.Stop()
			this.rtimer = nil
		}
		this.recvTimeout = timeout
		if timeout != 0 {
			deadline := time.Now().Add(timeout)
			if this.r.setDeadline(deadline) {
				this.rtimer = time.AfterFunc(timeout, this.onReadTimeout)
			}
		} else {
			this.r.setDeadline(time.Time{})
		}
	}
}

func (this *AIOConn) SetSendTimeout(timeout time.Duration) {
	this.muW.Lock()
	defer this.muW.Unlock()
	if atomic.LoadInt32(&this.closed) == 0 {
		if nil != this.wtimer {
			this.wtimer.Stop()
			this.wtimer = nil
		}
		this.sendTimeout = timeout
		if timeout != 0 {
			deadline := time.Now().Add(timeout)
			if this.w.setDeadline(deadline) {
				this.wtimer = time.AfterFunc(timeout, this.onWriteTimeout)
			}
		} else {
			this.w.setDeadline(time.Time{})
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
		if !this.r.empty() && !this.doingR {
			this.doingR = true
			this.muR.Unlock()
			this.service.deliverTask(&task{conn: this, tt: int64(EV_READ)})
		} else {
			this.muR.Unlock()
		}
	}

	if ev&EV_WRITE != 0 || ev&EV_ERROR != 0 {
		this.muW.Lock()
		this.writeable = true
		this.writeableVer++
		if !this.w.empty() && !this.doingW {
			this.doingW = true
			this.muW.Unlock()
			this.service.deliverTask(&task{conn: this, tt: int64(EV_WRITE)})
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

func (this *connMgr) isClosed() bool {
	this.Lock()
	defer this.Unlock()
	return this.closed
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
			tq:            make(chan task, TaskQueueSize),
			die:           make(chan struct{}),
		}

		runtime.SetFinalizer(s, func(s *AIOService) {
			s.Close()
		})

		for k, _ := range s.connMgr {
			s.connMgr[k].head.nnext = &s.connMgr[k].head
		}

		if worker <= 0 {
			worker = 1
		}

		for i := 0; i < worker; i++ {
			go func() {
				for {
					select {
					case <-s.die:
						return
					case v := <-s.tq:
						if int(v.tt) == EV_READ {
							v.conn.doRead()
						} else {
							v.conn.doWrite()
						}
					}
				}
			}()
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
			aioConnBase: aioConnBase{
				fd:        fd,
				rawconn:   conn,
				service:   this,
				sharebuff: option.ShareBuff,
				r:         newAioContextQueue(option.RecvqueSize),
				w:         newAioContextQueue(option.SendqueSize),
				userData:  option.UserData,
				tq:        this.tq,
				connMgr:   &this.connMgr[fd>>ConnMgrhashSize],
			},
		}

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

func (this *AIOService) deliverTask(t *task) bool {
	select {
	case <-this.die:
		return false
	case this.tq <- *t:
		return true
	}
}

func (this *AIOService) GetCompleteStatus() (r AIOResult, ok bool) {
	select {
	case <-this.die:
		ok = false
	default:
		r, ok = <-this.completeQueue
	}
	return
}

func (this *AIOService) Close() {
	this.closeOnce.Do(func() {
		runtime.SetFinalizer(this, nil)
		this.Lock()
		defer this.Unlock()

		close(this.die)

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
