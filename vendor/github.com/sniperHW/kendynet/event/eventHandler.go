package event

import (
	"github.com/sniperHW/kendynet"
	"github.com/sniperHW/kendynet/util"
	"sync"
	"sync/atomic"
)

type Handle *handle

type handList struct {
	head handle
	tail handle
}

type handle struct {
	pprev   *handle
	nnext   *handle
	fn      interface{}
	once    bool
	event   interface{}
	slot    *handlerSlot
	removed int32
	version int64
}

type handlerSlot struct {
	sync.Mutex
	l       []handList
	emiting bool
	removed int32
	current int
	version int64
}

type EventHandler struct {
	sync.RWMutex
	slots        map[interface{}]*handlerSlot
	processQueue *EventQueue
	version      int64
}

func NewEventHandler(processQueue ...*EventQueue) *EventHandler {
	var q *EventQueue
	if len(processQueue) > 0 {
		q = processQueue[0]
	}
	return &EventHandler{
		slots:        map[interface{}]*handlerSlot{},
		processQueue: q,
	}
}

func (this *EventHandler) register(event interface{}, once bool, fn interface{}) Handle {
	if nil == event {
		panic("event == nil")
	}

	switch fn.(type) {
	case func(), func(...interface{}), func(Handle), func(Handle, ...interface{}):
	default:
		panic("invaild fn type")
	}

	h := &handle{
		fn:    fn,
		once:  once,
		event: event,
	}

	this.Lock()
	defer this.Unlock()
	slot, ok := this.slots[event]
	if !ok {
		slot = &handlerSlot{
			l:       make([]handList, 2),
			version: atomic.AddInt64(&this.version, 1),
		}

		slot.l[0].head.nnext = &slot.l[0].tail
		slot.l[0].tail.pprev = &slot.l[0].head

		slot.l[1].head.nnext = &slot.l[1].tail
		slot.l[1].tail.pprev = &slot.l[1].head

		this.slots[h.event] = slot
	}
	slot.register(h)

	return Handle(h)
}

func (this *EventHandler) RegisterOnce(event interface{}, fn interface{}) Handle {
	return this.register(event, true, fn)
}

func (this *EventHandler) Register(event interface{}, fn interface{}) Handle {
	return this.register(event, false, fn)
}

func (this *EventHandler) Remove(h Handle) {
	hh := (*handle)(h)
	this.RLock()
	slot, ok := this.slots[hh.event]
	this.RUnlock()
	if ok {
		slot.remove(h)
	}
}

//触发事件
func (this *EventHandler) Emit(event interface{}, args ...interface{}) {
	this.RLock()
	slot, ok := this.slots[event]
	this.RUnlock()
	if ok {
		if this.processQueue != nil {
			this.processQueue.PostNoWait(slot.emit, args...)
		} else {
			slot.emit(args...)
		}
	}
}

func (this *EventHandler) Clear(event interface{}) {
	this.Lock()
	defer this.Unlock()
	slot, ok := this.slots[event]
	if ok {
		atomic.StoreInt32(&slot.removed, 1)
		delete(this.slots, event)
	}
}

func (this *handlerSlot) register(h *handle) {
	this.Lock()
	defer this.Unlock()
	l := &this.l[this.current]
	h.slot = this
	h.version = this.version

	(*l).tail.pprev.nnext = h
	h.nnext = &(*l).tail
	h.pprev = (*l).tail.pprev
	(*l).tail.pprev = h
}

func (this *handlerSlot) remove(h *handle) {
	if h.slot == this && h.version == this.version {
		atomic.StoreInt32(&h.removed, 1)
		this.Lock()
		if !this.emiting {
			h.pprev.nnext = h.nnext
			h.nnext.pprev = h.pprev
			h.slot = nil
			h.version = 0
		}
		this.Unlock()
	}
}

func pcall2(h *handle, args []interface{}) {
	defer util.Recover(kendynet.GetLogger())
	switch h.fn.(type) {
	case func():
		h.fn.(func())()
	case func(Handle):
		h.fn.(func(Handle))((Handle)(h))
	case func(...interface{}):
		h.fn.(func(...interface{}))(args...)
	case func(Handle, ...interface{}):
		h.fn.(func(Handle, ...interface{}))((Handle)(h), args...)
	}
}

func (this *handlerSlot) emit(args ...interface{}) {
	this.Lock()
	this.emiting = true
	l := &this.l[this.current]
	this.current = (this.current + 1) % 2
	this.Unlock()

	cur := (*l).head.nnext
	for cur != &(*l).tail {
		if atomic.LoadInt32(&this.removed) == 1 {
			//当前slot已经被清除
			return
		}
		next := cur.nnext
		if atomic.LoadInt32(&cur.removed) == 0 {
			pcall2(cur, args)
		}
		cur = next
	}

	this.Lock()

	ll := &this.l[this.current]

	cur = (*l).tail.pprev

	for cur != &(*l).head {
		pprev := cur.pprev
		if atomic.LoadInt32(&cur.removed) == 0 && !cur.once {
			cur.nnext = nil
			cur.pprev = nil
			(*ll).head.nnext.pprev = cur
			cur.nnext = (*ll).head.nnext
			cur.pprev = &(*ll).head
			(*ll).head.nnext = cur
		} else {
			cur.slot = nil
			cur.version = 0
		}
		cur = pprev
	}

	(*l).head.nnext = &(*l).tail
	(*l).tail.pprev = &(*l).head

	this.emiting = false
	this.Unlock()
}
