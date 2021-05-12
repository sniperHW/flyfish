package queue

import (
	"sync"
)

type ArrayQueue struct {
	list        []interface{}
	mtx         sync.Mutex
	emptyCond   *sync.Cond
	fullSize    int
	closed      bool
	emptyWaited int
}

func (self *ArrayQueue) Append(item interface{}) error {
	self.mtx.Lock()
	if self.closed {
		self.mtx.Unlock()
		return ErrQueueClosed
	}
	n := len(self.list)
	if n > self.fullSize {
		self.mtx.Unlock()
		return ErrQueueFull
	}

	self.list = append(self.list, item)
	needSignal := self.emptyWaited > 0
	self.mtx.Unlock()
	if needSignal {
		self.emptyCond.Signal()
	}
	return nil
}

func (self *ArrayQueue) ForceAppend(item interface{}) error {
	self.mtx.Lock()
	if self.closed {
		self.mtx.Unlock()
		return ErrQueueClosed
	}

	self.list = append(self.list, item)
	needSignal := self.emptyWaited > 0
	self.mtx.Unlock()
	if needSignal {
		self.emptyCond.Signal()
	}
	return nil
}

func (self *ArrayQueue) Pop(swaped []interface{}) (datas []interface{}, closed bool) {
	swaped = swaped[0:0]
	self.mtx.Lock()
	for !self.closed && len(self.list) == 0 {
		self.emptyWaited++
		self.emptyCond.Wait()
		self.emptyWaited--
	}
	datas = self.list
	closed = self.closed
	self.list = swaped
	self.mtx.Unlock()
	return
}

func (self *ArrayQueue) Closed() bool {
	var closed bool
	self.mtx.Lock()
	closed = self.closed
	self.mtx.Unlock()
	return closed
}

func (self *ArrayQueue) Close() bool {
	self.mtx.Lock()
	if self.closed {
		self.mtx.Unlock()
		return false
	}
	self.closed = true
	self.mtx.Unlock()
	self.emptyCond.Broadcast()
	return true
}

func (self *ArrayQueue) SetCap(newSize int) {
	if newSize > 0 {
		self.mtx.Lock()
		self.fullSize = newSize
		self.mtx.Unlock()
	}
}

func (self *ArrayQueue) Len() int {
	self.mtx.Lock()
	defer self.mtx.Unlock()
	return len(self.list)
}

func NewArrayQueue(cap ...int) *ArrayQueue {
	self := &ArrayQueue{}
	self.closed = false
	self.emptyCond = sync.NewCond(&self.mtx)
	self.list = make([]interface{}, 0, 64)

	if len(cap) > 0 {
		if cap[0] <= 0 {
			return nil
		}
		self.fullSize = cap[0]
	} else {
		self.fullSize = 128
	}

	return self
}
