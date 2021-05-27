package queue

import (
	"sync"
)

var (
	defaultFullSize = 10000
)

type listItem struct {
	ppnext *listItem
	v      interface{}
}

var listItemPool = sync.Pool{
	New: func() interface{} {
		return &listItem{}
	},
}

func getListItem(v interface{}) *listItem {
	l := listItemPool.Get().(*listItem)
	l.v = v
	return l
}

func releaseListItem(i *listItem) {
	i.v = nil
	listItemPool.Put(i)
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

	return &pq{
		priorityQueue: make([]list, priorityCount),
	}
}

func (this *pq) push(priority int, v interface{}) {
	if priority < 0 {
		priority = 0
	} else if priority >= len(this.priorityQueue) {
		priority = len(this.priorityQueue) - 1
	}

	this.priorityQueue[priority].push(getListItem(v))
	this.count++
	if priority > this.high {
		this.high = priority
	}
}

func (this *pq) pop() (ok bool, v interface{}) {
	if this.count > 0 {
		q := &this.priorityQueue[this.high]

		item := q.pop()
		v = item.v
		ok = true
		releaseListItem(item)

		this.count--

		for this.priorityQueue[this.high].empty() && this.high > 0 {
			this.high--
		}
	}
	return
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

//如果队列满返回ErrQueueFull
func (self *PriorityQueue) Append(priority int, item interface{}) error {
	self.listGuard.Lock()
	if self.closed {
		self.listGuard.Unlock()
		return ErrQueueClosed
	}

	n := self.q.count

	if n >= self.fullSize {
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

//忽略队列容量限制
func (self *PriorityQueue) ForceAppend(priority int, item interface{}) error {
	self.listGuard.Lock()
	if self.closed {
		self.listGuard.Unlock()
		return ErrQueueClosed
	}

	self.q.push(priority, item)

	needSignal := self.emptyWaited > 0
	self.listGuard.Unlock()
	if needSignal {
		self.emptyCond.Signal()
	}
	return nil
}

func (self *PriorityQueue) Pop() (closed bool, v interface{}) {
	self.listGuard.Lock()
	defer self.listGuard.Unlock()
	for !self.closed && self.q.count == 0 {
		self.emptyWaited++
		self.emptyCond.Wait()
		self.emptyWaited--
	}

	if self.q.count > 0 {
		_, v = self.q.pop()
	}

	closed = self.closed
	return
}

func (self *PriorityQueue) Close() {
	self.closeOnce.Do(func() {
		self.listGuard.Lock()
		self.closed = true
		self.listGuard.Unlock()
		self.emptyCond.Broadcast()
	})
}

func (self *PriorityQueue) Len() int {
	return self.q.count
}

func (self *PriorityQueue) Cap() int {
	return self.fullSize
}

func NewPriorityQueue(priorityCount int, fullSize ...int) *PriorityQueue {
	self := &PriorityQueue{}
	self.closed = false
	self.emptyCond = sync.NewCond(&self.listGuard)
	self.q = newpq(priorityCount)

	if len(fullSize) > 0 && fullSize[0] > 0 {
		self.fullSize = fullSize[0]
	} else {
		self.fullSize = defaultFullSize
	}

	return self
}
