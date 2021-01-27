package event

import (
	"errors"
	"sync"
)

var (
	ErrQueueClosed = errors.New("queue closed")
	ErrQueueFull   = errors.New("queue full")
)

const (
	defaultFullSize = 10000
)

type listItem struct {
	ppnext   *listItem
	v        interface{}
	poolItem bool
}

type list struct {
	tail *listItem
}

func (this *list) push(item *listItem) {
	var head *listItem
	if this.tail == nil {
		head = item
	} else {
		head = this.tail.ppnext
		this.tail.ppnext = item
	}
	item.ppnext = head
	this.tail = item
}

func (this *list) pop() *listItem {
	if this.tail == nil {
		return nil
	} else {
		item := this.tail.ppnext
		if item == this.tail {
			this.tail = nil
		} else {
			this.tail.ppnext = item.ppnext
		}

		item.ppnext = nil
		return item
	}
}

func (this *list) empty() bool {
	return this.tail == nil
}

type pq struct {
	priorityQueue []list
	itemPool      []listItem
	freelist      list
	count         int
	high          int //最高优先级的非空队列
}

/*
 *  创建一个优先级从0-priorityCount-1共priorityCount个优先级
 */
func newpq(priorityCount int) *pq {
	if priorityCount <= 0 {
		priorityCount = 1
	}

	q := &pq{
		priorityQueue: make([]list, priorityCount),
		itemPool:      make([]listItem, defaultFullSize, defaultFullSize),
	}

	for i, _ := range q.itemPool {
		item := &q.itemPool[i]
		item.poolItem = true
		q.freelist.push(item)
	}

	return q
}

func (this *pq) getItem(v interface{}) *listItem {
	item := this.freelist.pop()
	if nil == item {
		item = &listItem{}
	}
	item.v = v
	return item
}

func (this *pq) releaseItem(item *listItem) {
	if item.poolItem {
		this.freelist.push(item)
	}
}

func (this *pq) push(priority int, v interface{}) {
	if priority < 0 {
		priority = 0
	} else if priority >= len(this.priorityQueue) {
		priority = len(this.priorityQueue) - 1
	}

	this.priorityQueue[priority].push(this.getItem(v))
	this.count++
	if priority > this.high {
		this.high = priority
	}
}

func (this *pq) pop() (bool, interface{}) {
	if this.count == 0 {
		return false, nil
	} else {
		q := &this.priorityQueue[this.high]

		item := q.pop()
		v := item.v
		this.releaseItem(item)

		this.count--

		for this.priorityQueue[this.high].empty() && this.high > 0 {
			this.high--
		}

		return true, v
	}
}

type PriorityQueue struct {
	q           *pq
	listGuard   sync.Mutex
	emptyCond   *sync.Cond
	fullCond    *sync.Cond
	fullSize    int
	closed      bool
	emptyWaited int
	fullWaited  int
	closeOnce   sync.Once
}

func (self *PriorityQueue) AddNoWait(priority int, item interface{}, fullReturn ...bool) error {
	self.listGuard.Lock()
	if self.closed {
		self.listGuard.Unlock()
		return ErrQueueClosed
	}

	n := self.q.count

	if len(fullReturn) > 0 && fullReturn[0] && n >= self.fullSize {
		self.listGuard.Unlock()
		return ErrQueueFull
	}

	self.q.push(priority, item)

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

	for self.q.count >= self.fullSize {
		self.fullWaited++
		self.fullCond.Wait()
		self.fullWaited--
		if self.closed {
			self.listGuard.Unlock()
			return ErrQueueClosed
		}
	}

	self.q.push(priority, item)

	needSignal := self.emptyWaited > 0
	self.listGuard.Unlock()
	if needSignal {
		self.emptyCond.Signal()
	}
	return nil
}

func (self *PriorityQueue) Get() (closed bool, v interface{}) {
	self.listGuard.Lock()
	for !self.closed && self.q.count == 0 {
		//Cond.Wait不能设置超时，蛋疼
		self.emptyWaited++
		self.emptyCond.Wait()
		self.emptyWaited--
	}

	if self.q.count > 0 {
		_, v = self.q.pop()
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
	self.closeOnce.Do(func() {
		self.listGuard.Lock()
		self.closed = true
		self.listGuard.Unlock()
		self.emptyCond.Broadcast()
		self.fullCond.Broadcast()
	})
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

func NewPriorityQueue(priorityCount int, fullSize ...int) *PriorityQueue {
	self := &PriorityQueue{}
	self.closed = false
	self.emptyCond = sync.NewCond(&self.listGuard)
	self.fullCond = sync.NewCond(&self.listGuard)
	self.q = newpq(priorityCount)

	if len(fullSize) > 0 && fullSize[0] > 0 {
		self.fullSize = fullSize[0]
	} else {
		self.fullSize = defaultFullSize
	}

	return self
}
