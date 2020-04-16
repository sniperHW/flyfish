package aiogo

import (
	"errors"
	"fmt"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"unsafe"
)

var (
	ErrEof           = errors.New("EOF")
	ErrRecvTimeout   = errors.New("RecvTimeout")
	ErrSendTimeout   = errors.New("SendTimetout")
	ErrTimeout       = errors.New("Timeout")
	ErrConnClosed    = errors.New("conn closed")
	ErrWatcherClosed = errors.New("watcher closed")
	ErrUnsupportConn = errors.New("net.Conn does implement net.RawConn")
	ErrIoPending     = errors.New("io pending")
	ErrSendBuffNil   = errors.New("send buff is nil")
	ErrWatchFailed   = errors.New("watch failed")
)

const (
	EV_READ  = int(1 << 1)
	EV_WRITE = int(1 << 2)
	EV_ERROR = int(1 << 3)
)

type BufferPool interface {
	Get() []byte
	Put([]byte)
}

type opList struct {
	head op
	tail op
}

func (l *opList) push(o *op) {
	l.tail.pprev.nnext = o
	o.pprev = l.tail.pprev
	o.nnext = &l.tail
	l.tail.pprev = o
}

func (l *opList) front() *op {
	if l.head.nnext == &l.tail {
		return nil
	} else {
		return l.head.nnext
	}
}

type opPool struct {
	mu   spinLock
	head *op
	size int
	cap  int
}

var gOpPool *opPool = newOpPool(65536)

func newOpPool(initsize int) *opPool {
	p := &opPool{}
	for i := 0; i < initsize; i++ {
		p.put(&op{})
	}
	return p
}

func (this *opPool) get() *op {
	this.mu.Lock()
	if this.head == nil {
		this.mu.Unlock()
		return &op{}
	} else {
		o := this.head
		this.head = o.nnext
		this.size--
		this.mu.Unlock()
		o.nnext = nil
		return o
	}
}

func (this *opPool) put(o *op) {
	o.deadline = time.Time{}
	o.timer = nil
	o.buff = nil
	o.nextPos = 0
	o.nextBuff = 0
	o.size = 0
	o.done = nil
	o.ud = nil
	o.tt = 0
	o.c = nil
	o.nnext = nil
	o.pprev = nil
	this.mu.Lock()
	if this.size < this.cap {
		o.nnext = this.head
		this.head = o
		this.size++
	}
	this.mu.Unlock()
}

type op struct {
	idx      int //for timedHeap
	deadline time.Time
	timer    *timer
	buff     interface{}
	nextPos  int
	nextBuff int
	size     int
	done     *CompleteQueue
	ud       interface{}
	tt       int
	c        *Conn
	nnext    *op
	pprev    *op
}

func (o *op) remove() {
	o.pprev.nnext = o.nnext
	o.nnext.pprev = o.pprev
}

const max_send_size = 1024 * 256
const max_send_iovec_size = 512

type Conn struct {
	fd            int
	watchWrite    bool
	readable      bool
	writeable     bool
	pendingRead   opList
	pendingWrite  opList
	rawconn       net.Conn
	watcher       *Watcher
	worker        *worker
	ud            interface{}
	send_iovec    []syscall.Iovec
	closeOnce     sync.Once
	sendTimeout   atomic.Value
	recvTimeout   atomic.Value
	closed        int32
	pollerVersion int32
}

func (c *Conn) Watcher() *Watcher {
	return c.watcher
}

func (c *Conn) Fd() int {
	return c.fd
}

func (c *Conn) GetRowConn() net.Conn {
	return c.rawconn
}

func (c *Conn) SetRecvTimeout(timeout time.Duration) {
	c.recvTimeout.Store(timeout)
}

func (c *Conn) SetSendTimeout(timeout time.Duration) {
	c.sendTimeout.Store(timeout)
}

func (c *Conn) getRecvTimeout() time.Duration {
	t := c.recvTimeout.Load()
	if nil != t {
		return t.(time.Duration)
	} else {
		return 0
	}
}

func (c *Conn) getSendTimeout() time.Duration {
	t := c.sendTimeout.Load()
	if nil != t {
		return t.(time.Duration)
	} else {
		return 0
	}
}

func (c *Conn) doRead() {
	for op := c.pendingRead.front(); op != nil && c.readable && atomic.LoadInt32(&c.closed) == 0; op = c.pendingRead.front() {
		usePoolBuff := false
		useAllocBuff := false
		if nil == op.buff || 0 == len(op.buff.([]byte)) {
			var buff []byte
			if nil == c.watcher.bufferPool {
				op.buff = make([]byte, 4096)
				useAllocBuff = true
			} else {
				buff = c.watcher.bufferPool.Get()
				op.buff = buff[:cap(buff)]
				usePoolBuff = true
			}
		}

		for {
			size, err := syscall.Read(c.fd, op.buff.([]byte))

			if err == syscall.EINTR {
				continue
			} else if size == 0 || (err != nil && err != syscall.EAGAIN) {
				if usePoolBuff {
					c.watcher.bufferPool.Put(op.buff.([]byte))
					op.buff = nil
				} else if useAllocBuff {
					op.buff = nil
				}
				for op := c.pendingRead.front(); op != nil; op = c.pendingRead.front() {
					if nil != op.timer {
						op.timer.Remove(op)
					}
					op.remove()
					if atomic.LoadInt32(&c.closed) == 0 && nil != op.done {
						op.done.Post(&CompleteEvent{
							Conn: c,
							Type: Read,
							buff: op.buff,
							Size: 0,
							Err:  ErrEof,
							Ud:   op.ud,
						})
					}
					gOpPool.put(op)
				}
				return
			} else if err == syscall.EAGAIN {
				if usePoolBuff {
					c.watcher.bufferPool.Put(op.buff.([]byte))
					op.buff = nil
				} else if useAllocBuff {
					op.buff = nil
				}
				c.readable = false
				return
			} else {

				if size < len(op.buff.([]byte)) {
					c.readable = false
				}

				if nil != op.timer {
					op.timer.Remove(op)
				}

				op.remove()
				if atomic.LoadInt32(&c.closed) == 0 && nil != op.done {
					buff := op.buff.([]byte)
					op.buff = buff[:size]
					op.done.Post(&CompleteEvent{
						Conn: c,
						Type: Read,
						buff: op.buff,
						Size: size,
						Ud:   op.ud,
					})
				}
				gOpPool.put(op)
				break
			}
		}
	}
}

func (c *Conn) doWrite() {

	defer func() {
		if c.pendingWrite.front() == nil {
			if c.watchWrite && atomic.LoadInt32(&c.closed) == 0 && c.watcher.poller.disableWrite(c) {
				c.watchWrite = false
			}
		}
	}()

	for op := c.pendingWrite.front(); c.writeable && nil != op && atomic.LoadInt32(&c.closed) == 0; op = c.pendingWrite.front() {

		cc := 0
		total := 0

		for op != &c.pendingWrite.tail && total < max_send_size && cc < max_send_iovec_size {
			if op.tt == Write {
				b := op.buff.([]byte)
				size := len(b) - op.nextPos
				c.send_iovec[cc] = syscall.Iovec{&b[op.nextPos], uint64(size)}
				total += size
				cc++
			} else {
				buffs := op.buff.([][]byte)
				for i := op.nextBuff; i < len(buffs) && total < max_send_size && cc < max_send_iovec_size; cc++ {
					b := buffs[i]
					var pos int
					if i == op.nextBuff {
						pos = op.nextPos
					} else {
						pos = 0
					}
					size := len(b) - pos
					c.send_iovec[cc] = syscall.Iovec{&b[pos], uint64(size)}
					total += size
					i++
				}
			}
			op = op.nnext
		}

		for {

			nwRaw, _, errno := syscall.Syscall(syscall.SYS_WRITEV, uintptr(c.fd), uintptr(unsafe.Pointer(&c.send_iovec[0])), uintptr(cc))
			nw := int(nwRaw)

			if errno == syscall.EINTR {
				continue
			} else if errno != 0 && errno != syscall.EAGAIN {

				for op := c.pendingWrite.front(); nil != op; op = c.pendingWrite.front() {
					op.remove()
					if nil != op.timer {
						op.timer.Remove(op)
					}
					if atomic.LoadInt32(&c.closed) == 0 && nil != op.done {
						op.done.Post(&CompleteEvent{
							Conn: c,
							Type: op.tt,
							buff: op.buff,
							Size: 0,
							Err:  ErrEof,
							Ud:   op.ud,
						})
					}
					gOpPool.put(op)
				}
				return
			} else if errno == syscall.EAGAIN {
				c.writeable = false
				if !c.watchWrite {
					if c.watcher.poller.enableWrite(c) {
						c.watchWrite = true
					} else {
						if atomic.LoadInt32(&c.closed) == 0 {
							panic("enableWrite failed")
						}
					}
				}
				return
			} else {

				if nw < total {
					c.writeable = false
					if !c.watchWrite {
						if c.watcher.poller.enableWrite(c) {
							c.watchWrite = true
						} else {
							if atomic.LoadInt32(&c.closed) == 0 {
								panic("enableWrite failed")
							}
						}
					}
				}

				for nw > 0 {
					op := c.pendingWrite.front()
					if nil == op {
						str := fmt.Sprintf("nw:%d", nw)
						panic(str)
					}
					pos := op.nextPos
					if op.tt == Write {
						b := op.buff.([]byte)
						size := len(b) - pos
						if nw >= size {
							op.size += size
							nw -= size

							if nil != op.timer {
								op.timer.Remove(op)
							}
							op.remove()
							if atomic.LoadInt32(&c.closed) == 0 && nil != op.done {
								op.done.Post(&CompleteEvent{
									Conn: c,
									Type: op.tt,
									buff: op.buff,
									Size: op.size,
									Ud:   op.ud,
								})
							}
							gOpPool.put(op)
						} else {
							op.nextPos += nw
							op.size += nw
							nw = 0
						}
					} else {
						buffs := op.buff.([][]byte)
						for op.nextBuff < len(buffs) && nw > 0 {
							b := buffs[op.nextBuff]
							size := len(b) - pos
							if nw >= size {
								op.nextBuff++
								op.nextPos = 0
								pos = 0
								op.size += size
								nw -= size
							} else {
								op.nextPos += nw
								op.size += nw
								nw = 0
							}
						}

						if op.nextBuff == len(buffs) && op.nextPos == 0 {
							if nil != op.timer {
								op.timer.Remove(op)
							}
							op.remove()
							if atomic.LoadInt32(&c.closed) == 0 && nil != op.done {
								op.done.Post(&CompleteEvent{
									Conn: c,
									Type: op.tt,
									buff: op.buff,
									Size: op.size,
									Ud:   op.ud,
								})
							}
							gOpPool.put(op)
						}
					}
				}
				break
			}
		}
	}
}

func (c *Conn) onActive(event int) {

	if event&EV_READ != 0 || event&EV_ERROR != 0 {
		c.readable = true
		c.doRead()
	}

	if event&EV_WRITE != 0 || event&EV_ERROR != 0 {
		c.writeable = true
		c.doWrite()
	}
}

func (c *Conn) Recv(buff []byte, ud interface{}, done *CompleteQueue) error {
	if atomic.LoadInt32(&c.closed) == 1 {
		return ErrConnClosed
	}

	if atomic.LoadInt32(&c.watcher.closed) == 1 {
		return ErrWatcherClosed
	}

	op := gOpPool.get()
	op.buff = buff
	op.done = done
	op.ud = ud
	op.tt = Read
	op.c = c

	return c.worker.push(op)
}

func (c *Conn) Send(buff []byte, ud interface{}, done *CompleteQueue) error {

	if 0 == len(buff) {
		return ErrSendBuffNil
	}

	if atomic.LoadInt32(&c.closed) == 1 {
		return ErrConnClosed
	}

	if atomic.LoadInt32(&c.watcher.closed) == 1 {
		return ErrWatcherClosed
	}

	op := gOpPool.get()
	op.buff = buff
	op.done = done
	op.ud = ud
	op.tt = Write
	op.c = c

	return c.worker.push(op)
}

func (c *Conn) SendBuffers(buffs [][]byte, ud interface{}, done *CompleteQueue) error {

	if nil == buffs || 0 == len(buffs) {
		return ErrSendBuffNil
	}

	if atomic.LoadInt32(&c.closed) == 1 {
		return ErrConnClosed
	}

	if atomic.LoadInt32(&c.watcher.closed) == 1 {
		return ErrWatcherClosed
	}

	op := gOpPool.get()
	op.buff = buffs
	op.done = done
	op.ud = ud
	op.tt = WriteBuffs
	op.c = c

	return c.worker.push(op)
}

func (c *Conn) PostClosure(fn func()) error {
	op := gOpPool.get()
	op.ud = fn
	op.tt = userFunc
	return c.worker.push(op)
}

func (c *Conn) Close() {
	c.closeOnce.Do(func() {
		atomic.StoreInt32(&c.closed, 1)
		op := gOpPool.get()
		op.c = c
		op.tt = connClosed
		c.worker.push(op)
		runtime.SetFinalizer(c, nil)
		c.watcher.UnWatch(c)
		c.rawconn.Close()
	})
}

type Watcher struct {
	poller     pollerI
	stopOnce   sync.Once
	bufferPool BufferPool
	workers    []*worker
	closed     int32
	waitgroup  sync.WaitGroup
}

type WatcherOption struct {
	BufferPool  BufferPool
	WorkerCount int
}

func NewWatcher(option *WatcherOption) (*Watcher, error) {

	poller, err := openPoller()
	if nil != err {
		return nil, err
	}

	if nil == option {
		option = &WatcherOption{}
	}

	if option.WorkerCount <= 0 {
		option.WorkerCount = runtime.NumCPU()
	}

	w := &Watcher{
		poller:     poller,
		workers:    []*worker{},
		bufferPool: option.BufferPool,
	}

	for i := 0; i < option.WorkerCount; i++ {
		worker := newWorker()
		w.workers = append(w.workers, worker)
		w.waitgroup.Add(1)
		go worker.run(&w.waitgroup)
	}

	go w.poller.wait(&w.closed)

	return w, nil
}

func (w *Watcher) Watch(conn net.Conn) (*Conn, error) {

	if atomic.LoadInt32(&w.closed) == 1 {
		return nil, ErrWatcherClosed
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

	cc := &Conn{
		fd:         fd,
		readable:   false,
		writeable:  true,
		rawconn:    conn,
		watcher:    w,
		send_iovec: make([]syscall.Iovec, max_send_iovec_size),
		worker:     w.workers[fd%len(w.workers)],
	}

	cc.pendingRead.head.nnext = &cc.pendingRead.tail
	cc.pendingRead.tail.pprev = &cc.pendingRead.head

	cc.pendingWrite.head.nnext = &cc.pendingWrite.tail
	cc.pendingWrite.tail.pprev = &cc.pendingWrite.head

	if w.poller.watch(cc) {
		runtime.SetFinalizer(cc, func(cc *Conn) {
			cc.Close()
		})
		return cc, nil
	} else {
		return nil, ErrWatchFailed
	}

}

func (w *Watcher) UnWatch(c *Conn) {
	if c.watcher == w {
		w.poller.unwatch(c)
	}
}

func (w *Watcher) Close() {
	w.stopOnce.Do(func() {
		atomic.StoreInt32(&w.closed, 1)
		w.poller.trigger()
		for _, v := range w.workers {
			v.close()
		}
		w.waitgroup.Wait()
	})
}
