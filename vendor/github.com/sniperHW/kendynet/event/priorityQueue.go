package event

import (
	"container/heap"
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

type Item struct {
	Value    interface{}
	priority int
	seq      int
}

type pq []Item

func (this pq) Len() int { return len(this) }

func (this pq) Less(i, j int) bool {
	if this[i].priority == this[j].priority {
		return this[i].seq < this[j].seq
	} else {
		return this[i].priority > this[j].priority
	}
}

func (this pq) Swap(i, j int) {
	this[i], this[j] = this[j], this[i]
}

func (this *pq) Push(x interface{}) {
	item := x.(Item)
	*this = append(*this, item)
}

func (this *pq) Pop() interface{} {
	old := *this
	n := len(old)
	item := old[n-1]
	old[n-1].Value = nil
	*this = old[0 : n-1]
	return item
}

type PriorityQueue struct {
	list        pq
	listGuard   sync.Mutex
	emptyCond   *sync.Cond
	fullCond    *sync.Cond
	fullSize    int
	closed      bool
	emptyWaited int
	fullWaited  int
	name        string
}

func (self *PriorityQueue) AddNoWait(priority int, item interface{}, fullReturn ...bool) error {
	self.listGuard.Lock()
	if self.closed {
		self.listGuard.Unlock()
		return ErrQueueClosed
	}

	n := self.list.Len()

	if len(fullReturn) > 0 && fullReturn[0] && n >= self.fullSize {
		self.listGuard.Unlock()
		return ErrQueueFull
	}

	i := Item{
		priority: priority,
		Value:    item,
		seq:      self.list.Len(),
	}

	heap.Push(&self.list, i)

	needSignal := self.emptyWaited > 0
	self.listGuard.Unlock()
	if needSignal {
		self.emptyCond.Signal()
	}
	return nil
}

//如果队列满将会被阻塞
func (self *PriorityQueue) Add(priority int, item interface{}) error {
	self.listGuard.Lock()
	if self.closed {
		self.listGuard.Unlock()
		return ErrQueueClosed
	}

	for self.list.Len() >= self.fullSize {
		self.fullWaited++
		self.fullCond.Wait()
		self.fullWaited--
		if self.closed {
			self.listGuard.Unlock()
			return ErrQueueClosed
		}
	}

	i := Item{
		priority: priority,
		Value:    item,
		seq:      self.list.Len(),
	}

	heap.Push(&self.list, i)

	needSignal := self.emptyWaited > 0
	self.listGuard.Unlock()
	if needSignal {
		self.emptyCond.Signal()
	}
	return nil
}

func (self *PriorityQueue) Get() (closed bool, v interface{}) {
	self.listGuard.Lock()
	for !self.closed && self.list.Len() == 0 {
		//Cond.Wait不能设置超时，蛋疼
		self.emptyWaited++
		self.emptyCond.Wait()
		self.emptyWaited--
	}

	if self.list.Len() > 0 {
		v = heap.Pop(&self.list).(Item).Value
	}

	needSignal := self.fullWaited > 0
	closed = self.closed
	self.listGuard.Unlock()
	if needSignal {
		self.fullCond.Broadcast()
	}

	return
}

func (self *PriorityQueue) Close() {
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

func (self *PriorityQueue) SetFullSize(newSize int) {
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

func newPriorityQueue(name string, fullSize ...int) *PriorityQueue {
	self := &PriorityQueue{}
	self.name = name
	self.closed = false
	self.emptyCond = sync.NewCond(&self.listGuard)
	self.fullCond = sync.NewCond(&self.listGuard)
	self.list = make(pq, 0, initCap)

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

func NewPriorityQueueWithName(name string, fullSize ...int) *PriorityQueue {
	return newPriorityQueue(name, fullSize...)
}

func NewPriorityQueue(fullSize ...int) *PriorityQueue {
	return newPriorityQueue("", fullSize...)
}
