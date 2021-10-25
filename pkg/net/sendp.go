package net

import (
	"runtime"
	"sync"
)

const maxCacheItemCount = 4096

var gItemPool *sync.Pool = &sync.Pool{
	New: func() interface{} {
		return &listItem{}
	},
}

type listItem struct {
	nnext *listItem
	v     func()
}

type linkList struct {
	tail        *listItem
	count       int
	freeItems   *listItem
	freeCount   int
	mu          sync.Mutex
	cond        *sync.Cond
	emptyWaited int
}

func (this *linkList) pushItem(l **listItem, item *listItem) {
	var head *listItem
	if *l == nil {
		head = item
	} else {
		head = (*l).nnext
		(*l).nnext = item
	}
	item.nnext = head
	*l = item
}

func (this *linkList) popItem(l **listItem) *listItem {
	if *l == nil {
		return nil
	} else {
		item := (*l).nnext
		if item == (*l) {
			(*l) = nil
		} else {
			(*l).nnext = item.nnext
		}

		item.nnext = nil
		return item
	}
}

func (this *linkList) getPoolItem(v func()) *listItem {
	/*
	 * 本地的cache中获取listItem,如果没有才去sync.Pool取
	 */
	item := this.popItem(&this.freeItems)
	if nil == item {
		item = gItemPool.Get().(*listItem)
	} else {
		this.freeCount--
	}
	item.v = v
	return item
}

func (this *linkList) putPoolItem(item *listItem) {
	item.v = nil
	if this.freeCount < maxCacheItemCount {
		//如果尚未超过本地cache限制，将listItem放回本地cache供下次使用
		this.freeCount++
		this.pushItem(&this.freeItems, item)
	} else {
		gItemPool.Put(item)
	}
}

func (this *linkList) push(v func()) {
	this.mu.Lock()
	this.pushItem(&this.tail, this.getPoolItem(v))
	this.count++
	if this.emptyWaited > 0 {
		this.mu.Unlock()
		this.cond.Signal()
	} else {
		this.mu.Unlock()
	}
}

func (this *linkList) pop() func() {
	this.mu.Lock()
	for {
		item := this.popItem(&this.tail)
		if nil == item {
			this.emptyWaited++
			this.cond.Wait()
			this.emptyWaited--
		} else {
			this.count--
			v := item.v
			this.putPoolItem(item)
			this.mu.Unlock()
			return v
		}
	}
}

type sendp struct {
	taskQueue linkList
}

func (this *sendp) runTask(f func()) {
	this.taskQueue.push(f)
}

func (this *sendp) start() {
	go func() {
		for {
			this.taskQueue.pop()()
		}
	}()
}

var gSendP []sendp

func init() {
	gSendP = make([]sendp, runtime.NumCPU()*2)
	for k, _ := range gSendP {
		gSendP[k].taskQueue.cond = sync.NewCond(&gSendP[k].taskQueue.mu)
		gSendP[k].start()
	}
}
