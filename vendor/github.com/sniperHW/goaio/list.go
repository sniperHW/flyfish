package goaio

import (
	"sync"
)

const maxPoolItemCount = 4096

var gItemPool *sync.Pool = &sync.Pool{
	New: func() interface{} {
		return &listItem{}
	},
}

type listItem struct {
	nnext *listItem
	v     interface{}
}

type linkList struct {
	tail      *listItem
	itemPool  *listItem
	poolCount int
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

func (this *linkList) getPoolItem(v interface{}) *listItem {
	item := this.popItem(&this.itemPool)
	if nil == item {
		item = gItemPool.Get().(*listItem)
	} else {
		this.poolCount--
	}
	item.v = v
	return item
}

func (this *linkList) putPoolItem(item *listItem) {
	item.v = nil
	if this.poolCount < maxPoolItemCount {
		this.poolCount++
		this.pushItem(&this.itemPool, item)
	} else {
		gItemPool.Put(item)
	}
}

func (this *linkList) push(v interface{}) {
	this.pushItem(&this.tail, this.getPoolItem(v))
}

func (this *linkList) pop() interface{} {
	item := this.popItem(&this.tail)
	if nil == item {
		return nil
	} else {
		v := item.v
		this.putPoolItem(item)
		return v
	}
}
