package aiogo

import (
	"runtime"
	"sync"
	"sync/atomic"
)

type spinLock struct {
	lock int32
}

func (sp *spinLock) Lock() {
	i := 0
	for !atomic.CompareAndSwapInt32(&sp.lock, 0, 1) {
		i++
		if i >= 100 {
			runtime.Gosched()
		}
	}
}

func (sp *spinLock) Unlock() {
	atomic.StoreInt32(&sp.lock, 0)
}

const (
	Read       = 1 //读事件
	Write      = 2 //写事件
	WriteBuffs = 3
	User       = 4 //用户自定义事件
	//内部事件
	connClosed    = 5
	pollEvent     = 6
	timeoutNotify = 7
	userFunc      = 8
)

type CompleteEvent struct {
	next *CompleteEvent
	Type int
	Conn *Conn
	buff interface{}
	Size int
	Err  error
	Ud   interface{}
}

func (c *CompleteEvent) Next() *CompleteEvent {
	return c.next
}

func (c *CompleteEvent) GetBuff() []byte {
	if (c.Type == Read || c.Type == Write) && c.buff != nil {
		return c.buff.([]byte)
	} else {
		return nil
	}
}

func (c *CompleteEvent) GetBuffs() [][]byte {
	if c.Type == WriteBuffs && c.buff != nil {
		return c.buff.([][]byte)
	} else {
		return nil
	}
}

type CompleteQueue struct {
	head   *CompleteEvent
	tail   *CompleteEvent
	mu     sync.Locker
	cond   *sync.Cond
	waited int
	closed bool
}

func NewCompleteQueue() *CompleteQueue {
	q := &CompleteQueue{}
	q.closed = false
	q.mu = &spinLock{} //&sync.Mutex{}
	q.cond = sync.NewCond(q.mu)
	return q

}

func (c *CompleteQueue) Get() (*CompleteEvent, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for !c.closed && c.head == nil {
		c.waited++
		c.cond.Wait()
		c.waited--
	}

	if c.head != nil {
		head := c.head
		c.head = nil
		c.tail = nil
		return head, true
	} else {
		return nil, false
	}
}

func (c *CompleteQueue) Post(r *CompleteEvent) bool {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return false
	}

	if c.head == nil {
		c.head = r
		c.tail = r
	} else {
		c.tail.next = r
		c.tail = r
	}

	needSignal := c.waited > 0

	c.mu.Unlock()

	if needSignal {
		c.cond.Signal()
	}

	return true
}

func (c *CompleteQueue) Close() {

	c.mu.Lock()

	if c.closed {
		c.mu.Unlock()
		return
	}

	c.closed = true
	c.mu.Unlock()
	c.cond.Broadcast()
}
