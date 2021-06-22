package net

import (
	"errors"
	"sync"
)

var (
	ErrQueueClosed = errors.New("queue closed")
	ErrQueueFull   = errors.New("queue full")
)

const (
	initCap         = 64
	defaultFullSize = 10000
)

type SendQueue struct {
	list        []interface{}
	listGuard   sync.Mutex
	emptyCond   *sync.Cond
	fullCond    *sync.Cond
	fullSize    int
	closed      bool
	emptyWaited int
	fullWaited  int
}

//如果队列满返回busy
func (self *SendQueue) Add(item interface{}) error {
	self.listGuard.Lock()
	if self.closed {
		self.listGuard.Unlock()
		return ErrQueueClosed
	}

	if len(self.list) >= self.fullSize {
		self.listGuard.Unlock()
		return ErrQueueFull
	}

	self.list = append(self.list, item)

	needSignal := self.emptyWaited > 0
	self.listGuard.Unlock()
	if needSignal {
		self.emptyCond.Signal()
	}
	return nil
}

func (self *SendQueue) Get(swaped []interface{}) (closed bool, datas []interface{}) {
	swaped = swaped[0:0]
	self.listGuard.Lock()
	for !self.closed && len(self.list) == 0 {
		self.emptyWaited++
		self.emptyCond.Wait()
		self.emptyWaited--
	}
	datas = self.list
	closed = self.closed
	needSignal := self.fullWaited > 0
	self.list = swaped
	self.listGuard.Unlock()
	if needSignal {
		self.fullCond.Broadcast()
	}
	return
}

func (self *SendQueue) Close() (bool, int) {
	self.listGuard.Lock()
	n := len(self.list)
	if self.closed {
		self.listGuard.Unlock()
		return false, n
	}

	self.closed = true
	self.listGuard.Unlock()
	self.emptyCond.Broadcast()
	self.fullCond.Broadcast()

	return true, n
}

func (self *SendQueue) SetFullSize(newSize int) {
	if newSize > 0 {
		needSignal := false
		self.listGuard.Lock()
		oldSize := self.fullSize
		self.fullSize = newSize
		if oldSize < newSize && self.fullWaited > 0 {
			needSignal = true
		}
		self.listGuard.Unlock()
		if needSignal {
			self.fullCond.Broadcast()
		}
	}
}

func (self *SendQueue) Closed() bool {
	var closed bool
	self.listGuard.Lock()
	closed = self.closed
	self.listGuard.Unlock()
	return closed
}

func (self *SendQueue) Empty() bool {
	self.listGuard.Lock()
	defer self.listGuard.Unlock()
	return len(self.list) == 0
}

func NewSendQueue(fullSize ...int) *SendQueue {
	self := &SendQueue{}
	self.closed = false
	self.emptyCond = sync.NewCond(&self.listGuard)
	self.fullCond = sync.NewCond(&self.listGuard)
	self.list = make([]interface{}, 0, initCap)

	if len(fullSize) > 0 {
		if fullSize[0] <= 0 {
			return nil
		}
		self.fullSize = fullSize[0]
	} else {
		self.fullSize = defaultFullSize
	}

	return self
}
