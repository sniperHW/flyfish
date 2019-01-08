package util

import (
	"fmt"
	"sync"
)

var (
	ErrQueueClosed = fmt.Errorf("queue closed")
	ErrQueueFull   = fmt.Errorf("queue full")
)

type BlockQueue struct {
	list        []interface{}
	listGuard   sync.Mutex
	emptyCond   *sync.Cond
	fullCond    *sync.Cond
	fullSize    int
	closed      bool
	emptyWaited int
	fullWaited  int
	name        string
}

func (self *BlockQueue) AddNoWait(item interface{}, fullReturn ...bool) error {
	self.listGuard.Lock()
	if self.closed {
		self.listGuard.Unlock()
		return ErrQueueClosed
	}

	n := len(self.list)

	if len(fullReturn) > 0 && fullReturn[0] && n >= self.fullSize {
		self.listGuard.Unlock()
		return ErrQueueFull
	}

	self.list = append(self.list, item)

	needSignal := self.emptyWaited > 0 && n == 0 /*BlockQueue目前主要用于单消费者队列，这里n == 0的处理是为了这种情况的优化,减少Signal的调用次数*/
	self.listGuard.Unlock()
	if needSignal {
		self.emptyCond.Signal()
	}
	return nil
}

//如果队列满将会被阻塞
func (self *BlockQueue) Add(item interface{}) error {
	self.listGuard.Lock()
	if self.closed {
		self.listGuard.Unlock()
		return ErrQueueClosed
	}

	for len(self.list) >= self.fullSize {
		self.fullWaited++
		fmt.Println("wait full", self.name, self.fullSize)
		self.fullCond.Wait()
		fmt.Println("wait full awake", self.name)
		self.fullWaited--
		if self.closed {
			self.listGuard.Unlock()
			return ErrQueueClosed
		}
	}

	n := len(self.list)
	self.list = append(self.list, item)

	needSignal := self.emptyWaited > 0 && n == 0 /*BlockQueue目前主要用于单消费者队列，这里n == 0的处理是为了这种情况的优化,减少Signal的调用次数*/
	self.listGuard.Unlock()
	if needSignal {
		self.emptyCond.Signal()
	}
	return nil
}

func (self *BlockQueue) Closed() bool {
	var closed bool
	self.listGuard.Lock()
	closed = self.closed
	self.listGuard.Unlock()
	return closed
}

func (self *BlockQueue) Get() (closed bool, datas []interface{}) {
	self.listGuard.Lock()
	for !self.closed && len(self.list) == 0 {
		//Cond.Wait不能设置超时，蛋疼
		self.emptyWaited++
		self.emptyCond.Wait()
		self.emptyWaited--
	}
	if len(self.list) > 0 {
		datas = self.list
		self.list = make([]interface{}, 0)
	}
	needSignal := self.fullWaited > 0
	closed = self.closed
	self.listGuard.Unlock()
	if needSignal {
		self.fullCond.Broadcast()
	}
	return
}

func (self *BlockQueue) Swap(swaped []interface{}) (closed bool, datas []interface{}) {
	swaped = swaped[0:0]
	self.listGuard.Lock()
	for !self.closed && len(self.list) == 0 {
		self.emptyWaited++
		//Cond.Wait不能设置超时，蛋疼
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

func (self *BlockQueue) Close() {
	self.listGuard.Lock()

	if self.closed {
		self.listGuard.Unlock()
		return
	}

	self.closed = true
	self.listGuard.Unlock()
	self.emptyCond.Signal()
	self.fullCond.Broadcast()
}

func (self *BlockQueue) Len() (length int) {
	self.listGuard.Lock()
	length = len(self.list)
	self.listGuard.Unlock()
	return
}

func (self *BlockQueue) Clear() {
	self.listGuard.Lock()
	self.list = self.list[0:0]
	self.listGuard.Unlock()
	return
}

func NewBlockQueueWithName(name string, fullSize ...int) *BlockQueue {
	self := &BlockQueue{}
	self.name = name
	self.closed = false
	self.emptyCond = sync.NewCond(&self.listGuard)
	self.fullCond = sync.NewCond(&self.listGuard)

	if len(fullSize) > 0 {
		if fullSize[0] <= 0 {
			return nil
		}
		self.fullSize = fullSize[0]
	} else {
		self.fullSize = 10000
	}

	return self
}

func NewBlockQueue(fullSize ...int) *BlockQueue {
	self := &BlockQueue{}
	self.closed = false
	self.emptyCond = sync.NewCond(&self.listGuard)
	self.fullCond = sync.NewCond(&self.listGuard)

	if len(fullSize) > 0 {
		if fullSize[0] <= 0 {
			return nil
		}
		self.fullSize = fullSize[0]
	} else {
		self.fullSize = 10000
	}

	return self
}
