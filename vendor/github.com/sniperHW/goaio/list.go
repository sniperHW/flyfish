package goaio

import (
	"sync"
)

type listItem struct {
	nnext *listItem
	v     interface{}
}

var itemPool *sync.Pool = &sync.Pool{
	New: func() interface{} {
		return &listItem{}
	},
}

func getItem(v interface{}) (i *listItem) {
	i = itemPool.Get().(*listItem)
	i.v = v
	return i
}

func putItem(i *listItem) {
	i.v = nil
	itemPool.Put(i)
}

type linkList struct {
	tail *listItem
}

/*
func (l *linkList) push(v interface{}) {
	n := getItem(v)
	if l.head == nil {
		l.head = n
	} else {
		l.tail.nnext = n
	}
	l.tail = n
}

func (l *linkList) pop() interface{} {
	if l.head == nil {
		return nil
	} else {
		first := l.head
		l.head = first.nnext
		if l.head == nil {
			l.tail = nil
		}
		v := first.v
		putItem(first)
		return v
	}
}
*/

func (this *linkList) push(v interface{}) {
	item := getItem(v)
	var head *listItem
	if this.tail == nil {
		head = item
	} else {
		head = this.tail.nnext
		this.tail.nnext = item
	}
	item.nnext = head
	this.tail = item
}

func (this *linkList) pop() interface{} {
	if this.tail == nil {
		return nil
	} else {
		item := this.tail.nnext
		if item == this.tail {
			this.tail = nil
		} else {
			this.tail.nnext = item.nnext
		}

		item.nnext = nil
		v := item.v
		putItem(item)
		return v
	}
}
