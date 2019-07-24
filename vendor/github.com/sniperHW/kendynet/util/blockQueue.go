package util

import (
	"fmt"
	"sync"
)

var (
	ErrQueueClosed = fmt.Errorf("queue closed")
	ErrQueueFull   = fmt.Errorf("queue full")
)

const (
	initCap         = 64
	defaultFullSize = 10000
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

	needSignal := self.emptyWaited > 0 && n == 0
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

	needSignal := self.emptyWaited > 0 && n == 0
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
		self.list = make([]interface{}, 0, initCap)
	}
	needSignal := self.fullWaited > 0 && len(datas) >= self.fullSize
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
	needSignal := self.fullWaited > 0 && len(datas) >= self.fullSize
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
	self.emptyCond.Broadcast()
	self.fullCond.Broadcast()
}

func (self *BlockQueue) Len() int {
	self.listGuard.Lock()
	defer self.listGuard.Unlock()
	return len(self.list)
}

func (self *BlockQueue) Full() bool {
	self.listGuard.Lock()
	defer self.listGuard.Unlock()
	return len(self.list) >= self.fullSize
}

func (self *BlockQueue) Clear() {
	self.listGuard.Lock()
	defer self.listGuard.Unlock()
	self.list = self.list[0:0]
}

func (self *BlockQueue) SetFullSize(newSize int) {
	self.listGuard.Lock()
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

func NewBlockQueueWithName(name string, fullSize ...int) *BlockQueue {
	self := &BlockQueue{}
	self.name = name
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

func NewBlockQueue(fullSize ...int) *BlockQueue {
	self := &BlockQueue{}
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
