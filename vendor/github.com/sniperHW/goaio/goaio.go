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
	ErrEof                = errors.New("EOF")
	ErrRecvTimeout        = errors.New("RecvTimeout")
	ErrSendTimeout        = errors.New("SendTimetout")
	ErrConnClosed         = errors.New("conn closed")
	ErrServiceClosed      = errors.New("service closed")
	ErrUnsupportConn      = errors.New("net.Conn does implement net.RawConn")
	ErrBusy               = errors.New("busy")
	ErrWatchFailed        = errors.New("watch failed")
	ErrActiveClose        = errors.New("active close")
	ErrCloseNone          = errors.New("close no reason")
	ErrCloseGC            = errors.New("close by gc")
	ErrCloseServiceClosed = errors.New("close because of service closed")
)

const (
	EV_READ  = int(1 << 1)
	EV_WRITE = int(1 << 2)
	EV_ERROR = int(1 << 3)
)

var (
	DefaultWorkerCount = 1
)

type ShareBuffer interface {
	Acquire() []byte
	Release([]byte)
}

type AIOConnOption struct {
	SendqueSize int
	RecvqueSize int
	ShareBuff   ShareBuffer
	UserData    interface{}
}

type AIOConn struct {
	sync.Mutex
	nnext         *AIOConn
	fd            int
	rawconn       net.Conn
	readable      bool
	readableVer   int
	writeable     bool
	writeableVer  int
	w             *aioContextQueue
	r             *aioContextQueue
	service       *AIOService
	doing         bool
	closed        bool
	pollerVersion int32
	closeOnce     sync.Once
	sendTimeout   time.Duration
	recvTimeout   time.Duration
	timer         *Timer
	reason        error
	sharebuff     ShareBuffer
	userData      interface{}
	doTimeout     *Timer
	tqIdx         int
}

type aioContext struct {
	buff     []byte
	offset   int
	context  interface{}
	deadline time.Time
}

type aioContextQueue struct {
	head  int
	tail  int
	queue []aioContext
}

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

	if (this.tail+1)%len(this.queue) == this.head {
		return ErrBusy
	} else {
		this.queue[this.tail] = c
		this.tail = (this.tail + 1) % len(this.queue)
		return nil
	}
}

func (this *aioContextQueue) dropLast() {
	if this.head != this.tail {
		this.queue[this.tail].buff = nil
		this.queue[this.tail].context = nil
		if this.tail == 0 {
			this.tail = len(this.queue) - 1
		} else {
			this.tail--
		}
	}
}

func (this *aioContextQueue) front() *aioContext {
	return &this.queue[this.head]
}

func (this *aioContextQueue) popFront() {
	if this.head != this.tail {
		this.queue[this.head].buff = nil
		this.queue[this.head].context = nil
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

type AIOResult struct {
	ppnext        *AIOResult
	Conn          *AIOConn
	Buff          []byte
	Context       interface{}
	Err           error
	Bytestransfer int
}

type aioResultList struct {
	head *AIOResult
}

func (this *aioResultList) empty() bool {
	return this.head == nil
}

func (this *aioResultList) push(item *AIOResult) {
	var head *AIOResult
	if this.head == nil {
		head = item
	} else {
		head = this.head.ppnext
		this.head.ppnext = item
	}
	item.ppnext = head
	this.head = item
}

func (this *aioResultList) pop() *AIOResult {
	if this.head == nil {
		return nil
	} else {
		item := this.head.ppnext
		if item == this.head {
			this.head = nil
		} else {
			this.head.ppnext = item.ppnext
		}

		item.ppnext = nil
		return item
	}
}

type completetionQueue struct {
	mu            sync.Mutex
	cond          *sync.Cond
	completeQueue aioResultList
	freeList      aioResultList
	closed        bool
	waitCount     int
}

func newCompletetionQueue() *completetionQueue {
	q := &completetionQueue{}
	q.cond = sync.NewCond(&q.mu)
	return q
}

func (this *completetionQueue) close() {
	this.mu.Lock()
	this.closed = true
	this.mu.Unlock()
	this.cond.Broadcast()
}

func (this *completetionQueue) postCompleteStatus(c *AIOConn, buff []byte, bytestransfer int, err error, context interface{}) {
	this.mu.Lock()
	if this.closed {
		this.mu.Unlock()
	} else {

		r := this.freeList.pop()
		if nil == r {
			r = &AIOResult{}
		}

		r.Conn = c
		r.Context = context
		r.Err = err
		r.Buff = buff
		r.Bytestransfer = bytestransfer

		this.completeQueue.push(r)

		waitCount := this.waitCount

		this.mu.Unlock()

		if waitCount > 0 {
			this.cond.Signal()
		}
	}
}

func (this *completetionQueue) getCompleteStatus() (res AIOResult, err error) {
	this.mu.Lock()

	for this.completeQueue.empty() {
		if this.closed {
			err = ErrServiceClosed
			this.mu.Unlock()
			return
		}
		this.waitCount++
		this.cond.Wait()
		this.waitCount--
	}

	e := this.completeQueue.pop()
	res = *e
	e.Buff = nil
	e.Context = nil
	e.Conn = nil

	this.freeList.push(e)

	this.mu.Unlock()

	return
}

func (this *AIOConn) GetUserData() interface{} {
	return this.userData
}

func (this *AIOConn) Close(reason error) {
	this.closeOnce.Do(func() {
		runtime.SetFinalizer(this, nil)
		this.Lock()
		defer this.Unlock()
		this.closed = true
		if nil == reason {
			reason = ErrCloseNone
		}
		this.reason = reason

		if nil != this.timer {
			this.timer.Cancel()
			this.timer = nil
		}
		if !this.doing {
			this.doing = true
			if nil != this.service.pushIOTask(this) {
				this.rawconn.Close()
			}
		}
	})
}

func (this *AIOConn) processTimeout() {
	now := time.Now()
	if this.recvTimeout > 0 {
		for !this.r.empty() {
			f := this.r.front()
			if now.After(f.deadline) {
				this.service.postCompleteStatus(this, f.buff, 0, ErrRecvTimeout, f.context)
				this.r.popFront()
			} else {
				break
			}
		}
	}

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
	}

	var deadline time.Time
	if this.recvTimeout > 0 && !this.r.empty() && this.r.front().deadline.After(deadline) {
		deadline = this.r.front().deadline
	}

	if this.sendTimeout > 0 && !this.w.empty() && this.w.front().deadline.After(deadline) {
		deadline = this.w.front().deadline
	}

	if !deadline.IsZero() {
		this.timer = newTimer(now.Sub(deadline), this.onTimeout)
	}
}

func (this *AIOConn) onTimeout(t *Timer) {
	this.Lock()
	defer this.Unlock()
	if this.timer == t {
		this.doTimeout = this.timer
		if !this.doing {
			this.doing = true
			this.service.pushIOTask(this)
		}
	}
}

func (this *AIOConn) SetRecvTimeout(timeout time.Duration) {
	this.Lock()
	defer this.Unlock()
	if !this.closed {
		if nil != this.timer {
			this.timer.Cancel()
			this.timer = nil
		}
		this.recvTimeout = timeout
		if timeout != 0 {
			deadline := time.Now().Add(timeout)
			if this.r.setDeadline(deadline) {
				this.timer = newTimer(timeout, this.onTimeout)
			}
		} else {
			this.r.setDeadline(time.Time{})
		}
	}
}

func (this *AIOConn) SetSendTimeout(timeout time.Duration) {
	this.Lock()
	defer this.Unlock()
	if !this.closed {
		this.sendTimeout = timeout
		if timeout != 0 {
			deadline := time.Now().Add(timeout)
			if this.w.setDeadline(deadline) {
				this.timer = newTimer(timeout, this.onTimeout)
			}
		} else {
			this.w.setDeadline(time.Time{})
			if nil != this.timer {
				this.timer.Cancel()
				this.timer = nil
			}
		}
	}
}

func (this *AIOConn) getRecvTimeout() time.Duration {
	return this.recvTimeout
}

func (this *AIOConn) getSendTimeout() time.Duration {
	return this.sendTimeout
}

func (this *AIOConn) canRead() bool {
	return this.readable && !this.r.empty()
}

func (this *AIOConn) canWrite() bool {
	return this.writeable && !this.w.empty()
}

func (this *AIOConn) Send(buff []byte, context interface{}) error {
	if atomic.LoadInt32(this.service.closed) == 1 {
		return ErrServiceClosed
	}

	this.Lock()
	defer this.Unlock()

	if this.closed {
		return ErrConnClosed
	} else {

		var deadline time.Time

		timeout := this.getSendTimeout()

		if 0 != timeout {
			deadline = time.Now().Add(timeout)
		}

		if err := this.w.add(aioContext{
			buff:     buff,
			context:  context,
			deadline: deadline,
		}); nil != err {
			return err
		}

		if !this.service.addIO(this) {
			this.w.dropLast()
			return ErrServiceClosed
		}

		if !deadline.IsZero() && nil == this.timer {
			this.timer = newTimer(timeout, this.onTimeout)
		}

		if this.writeable && !this.doing {
			this.doing = true
			this.service.pushIOTask(this)
		}

		return nil
	}
}

func (this *AIOConn) Recv(buff []byte, context interface{}) error {
	if atomic.LoadInt32(this.service.closed) == 1 {
		return ErrServiceClosed
	}

	this.Lock()
	defer this.Unlock()

	if this.closed {
		return ErrConnClosed
	} else {

		var deadline time.Time

		timeout := this.getRecvTimeout()

		if 0 != timeout {
			deadline = time.Now().Add(timeout)
		}

		if err := this.r.add(aioContext{
			buff:     buff,
			context:  context,
			deadline: deadline,
		}); nil != err {
			return err
		}

		if !this.service.addIO(this) {
			this.r.dropLast()
			return ErrServiceClosed
		}

		if !deadline.IsZero() && nil == this.timer {
			this.timer = newTimer(timeout, this.onTimeout)
		}

		if this.readable && !this.doing {
			this.doing = true
			this.service.pushIOTask(this)
		}

		return nil
	}
}

func (this *AIOConn) onActive(ev int) {

	this.Lock()
	defer this.Unlock()

	if ev&EV_READ != 0 || ev&EV_ERROR != 0 {
		this.readable = true
		this.readableVer++
	}

	if ev&EV_WRITE != 0 || ev&EV_ERROR != 0 {
		this.writeable = true
		this.writeableVer++
	}

	if (this.canRead() || this.canWrite()) && !this.doing {
		this.doing = true
		this.service.pushIOTask(this)
	}
}

func (this *AIOConn) doRead() {
	c := this.r.front()
	ver := this.readableVer
	this.Unlock()
	var buff []byte
	if nil != this.sharebuff {
		buff = this.sharebuff.Acquire()
	} else {
		buff = c.buff
	}

	size, err := syscall.Read(this.fd, buff)
	this.Lock()
	if err == syscall.EINTR {
		return
	} else if size == 0 || (err != nil && err != syscall.EAGAIN) {
		if size == 0 {
			err = ErrEof
		}

		if nil != this.sharebuff {
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

		if nil != this.sharebuff {
			this.sharebuff.Release(buff)
		}

	} else {
		this.service.postCompleteStatus(this, buff, size, nil, c.context)
		this.r.popFront()
	}
}

func (this *AIOConn) doWrite() {
	c := this.w.front()
	this.Unlock()
	ver := this.writeableVer
	size, err := syscall.Write(this.fd, c.buff[c.offset:])
	this.Lock()
	if err == syscall.EINTR {
		return
	} else if err != nil && err != syscall.EAGAIN {
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
		if len(c.buff[c.offset:]) == size {
			this.service.postCompleteStatus(this, c.buff, len(c.buff), nil, c.context)
			this.w.popFront()
		} else {
			c.offset += size
			if ver == this.writeableVer {
				this.writeable = false
			}
		}
	}
}

func (this *AIOConn) Do() {
	this.Lock()
	defer this.Unlock()
	for {
		if this.closed {
			for !this.r.empty() {
				c := this.r.front()
				this.service.postCompleteStatus(this, c.buff, 0, this.reason, c.context)
				this.r.popFront()
			}
			for !this.w.empty() {
				c := this.w.front()
				this.service.postCompleteStatus(this, c.buff, c.offset, this.reason, c.context)
				this.w.popFront()
			}
			this.service.unwatch(this)
			this.rawconn.Close()
			return
		} else {

			if nil != this.timer && this.timer == this.doTimeout {
				this.doTimeout = nil
				this.processTimeout()
			}

			if this.canRead() {
				this.doRead()
			}

			if this.canWrite() {
				this.doWrite()
			}

			if !(this.closed || this.canRead() || this.canWrite()) {
				break
			}
		}
	}

	this.doing = false
}

type AIOService struct {
	sync.Mutex
	completeQueue *completetionQueue
	tq            []*taskQueue
	poller        pollerI
	closed        *int32
	waitgroup     *sync.WaitGroup
	closeOnce     sync.Once
	connMgr       []*connMgr
}

type connMgr struct {
	sync.Mutex
	conns  map[*AIOConn]int
	closed bool
}

func (this *connMgr) addIO(c *AIOConn) bool {
	this.Lock()
	defer this.Unlock()
	if !this.closed {
		this.conns[c] = this.conns[c] + 1
		return true
	} else {
		return false
	}
}

func (this *connMgr) subIO(c *AIOConn) {
	this.Lock()
	defer this.Unlock()
	count := this.conns[c] - 1
	if count == 0 {
		delete(this.conns, c)
	} else {
		this.conns[c] = count
	}
}

func (this *connMgr) close() {
	this.Lock()
	conns := []*AIOConn{}
	for v, _ := range this.conns {
		conns = append(conns, v)
	}
	this.Unlock()

	for _, v := range conns {
		v.Close(ErrCloseServiceClosed)
	}
}

func NewAIOService(worker int) *AIOService {
	if poller, err := openPoller(); nil == err {
		waitgroup := &sync.WaitGroup{}
		s := &AIOService{}
		s.completeQueue = newCompletetionQueue()
		s.poller = poller
		s.connMgr = make([]*connMgr, 127)
		s.waitgroup = waitgroup
		s.closed = new(int32)
		for k, _ := range s.connMgr {
			s.connMgr[k] = &connMgr{
				conns: map[*AIOConn]int{},
			}
		}

		if worker <= 0 {
			worker = 1
		}

		for i := 0; i < worker; i++ {
			tq := NewTaskQueue()
			s.tq = append(s.tq, tq)
			go func() {
				waitgroup.Add(1)
				defer waitgroup.Done()
				for {
					head, err := tq.pop()
					if nil != err {
						return
					} else {
						for head != nil {
							next := head.nnext
							head.Do()
							head = next
						}
					}
				}
			}()
		}

		go poller.wait(s.closed)

		runtime.SetFinalizer(s, func(s *AIOService) {
			s.Close()
		})

		return s
	} else {
		return nil
	}
}

func (this *AIOService) unwatch(c *AIOConn) {
	this.poller.unwatch(c)
}

func (this *AIOService) addIO(c *AIOConn) bool {
	return this.connMgr[c.fd%len(this.connMgr)].addIO(c)
}

func (this *AIOService) subIO(c *AIOConn) {
	this.connMgr[c.fd%len(this.connMgr)].subIO(c)
}

func (this *AIOService) Bind(conn net.Conn, option AIOConnOption) (*AIOConn, error) {

	this.Lock()
	defer this.Unlock()

	if 1 == *this.closed {
		return nil, ErrServiceClosed
	}

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
		r:         newAioContextQueue(option.RecvqueSize),
		w:         newAioContextQueue(option.SendqueSize),
		userData:  option.UserData,
		//todo:根据各tq的负载情况动态调整tqIdx以平衡worker线程的工作负载
		tqIdx: fd % len(this.tq),
	}

	if this.poller.watch(cc) {
		runtime.SetFinalizer(cc, func(cc *AIOConn) {
			cc.Close(ErrCloseGC)
		})
		return cc, nil
	} else {
		return nil, ErrWatchFailed
	}
}

func (this *AIOService) pushIOTask(c *AIOConn) error {
	return this.tq[c.tqIdx].push(c)
}

func (this *AIOService) postCompleteStatus(c *AIOConn, buff []byte, bytestransfer int, err error, context interface{}) {
	this.subIO(c)
	this.completeQueue.postCompleteStatus(c, buff, bytestransfer, err, context)
}

func (this *AIOService) GetCompleteStatus() (AIOResult, error) {
	return this.completeQueue.getCompleteStatus()
}

func (this *AIOService) Close() {
	this.closeOnce.Do(func() {
		runtime.SetFinalizer(this, nil)
		this.Lock()
		defer this.Unlock()
		atomic.StoreInt32(this.closed, 1)
		this.poller.close()
		for _, v := range this.connMgr {
			v.close()
		}

		for _, v := range this.tq {
			v.close()
		}

		//等待worker处理完所有的AIOConn清理
		this.waitgroup.Wait()

		//所有的待处理的AIO在此时已经被投递到completeQueue，可以关闭completeQueue。
		this.completeQueue.close()
	})
}

var defalutService *AIOService
var createOnce sync.Once

func Bind(conn net.Conn, option AIOConnOption) (*AIOConn, error) {
	createOnce.Do(func() {
		defalutService = NewAIOService(DefaultWorkerCount)
	})
	return defalutService.Bind(conn, option)
}

func GetCompleteStatus() (AIOResult, error) {
	createOnce.Do(func() {
		defalutService = NewAIOService(DefaultWorkerCount)
	})
	return defalutService.GetCompleteStatus()
}
