package event

import (
	"github.com/sniperHW/kendynet"
	"github.com/sniperHW/kendynet/util"
	"reflect"
	"sync"
	"sync/atomic"
)

type Handle int64

type handList struct {
	head handle
	tail handle
}

var handlePool = sync.Pool{
	New: func() interface{} {
		return &handle{}
	},
}

func get() *handle {
	h := handlePool.Get().(*handle)
	h.status = status_ok
	return h
}

func put(h *handle) {
	handlePool.Put(h)
}

const (
	status_ok       = 0
	status_remove   = 1
	status_register = 2
)

type handle struct {
	h      Handle
	pprev  *handle
	nnext  *handle
	fn     interface{}
	once   bool
	event  interface{}
	slot   *handlerSlot
	status int
}

type handlerSlot struct {
	l       handList
	emiting bool
	eh      *EventHandler
}

type EventHandler struct {
	mtx          sync.Mutex
	c            int64
	slots        map[interface{}]*handlerSlot
	hmap         map[Handle]*handle
	processQueue *EventQueue
}

func NewEventHandler(processQueue ...*EventQueue) *EventHandler {
	var q *EventQueue
	if len(processQueue) > 0 {
		q = processQueue[0]
	}
	return &EventHandler{
		slots:        map[interface{}]*handlerSlot{},
		hmap:         map[Handle]*handle{},
		processQueue: q,
	}
}

func (this *EventHandler) register_(h *handle) {
	this.hmap[h.h] = h
	slot, ok := this.slots[h.event]
	if !ok {
		slot = &handlerSlot{}
		slot.eh = this
		slot.l.head.nnext = &slot.l.tail
		slot.l.tail.pprev = &slot.l.head
		this.slots[h.event] = slot
	}
	slot.register(h)
}

func (this *EventHandler) register(event interface{}, once bool, fn interface{}) Handle {
	if nil == event {
		panic("event == nil")
	}

	switch fn.(type) {
	case func():
		break
	case func(...interface{}):
		break
	case func(Handle):
		break
	case func(Handle, ...interface{}):
		break
	default:
		panic("invaild fn type")
		break
	}

	h := get()
	h.fn = fn
	h.once = once
	h.event = event
	h.h = Handle(atomic.AddInt64(&this.c, 1))

	this.mtx.Lock()
	this.register_(h)
	this.mtx.Unlock()

	return h.h
}

func (this *EventHandler) RegisterOnce(event interface{}, fn interface{}) Handle {
	return this.register(event, true, fn)
}

func (this *EventHandler) Register(event interface{}, fn interface{}) Handle {
	return this.register(event, false, fn)
}

func (this *EventHandler) remove_(h Handle) {
	hh := this.hmap[h]
	if nil != hh {
		slot, ok := this.slots[hh.event]
		if ok {
			if slot.emiting {
				hh.status = status_remove
			} else {
				slot.remove(hh)
			}
		}
	}
}

func (this *EventHandler) Remove(h Handle) {
	this.mtx.Lock()
	defer this.mtx.Unlock()
	this.remove_(h)
}

func (this *EventHandler) clear_(event interface{}) {
	slot, ok := this.slots[event]
	if ok {
		slot.clear()
	}
}

func (this *EventHandler) Clear(event interface{}) {
	this.mtx.Lock()
	defer this.mtx.Unlock()
	this.clear_(event)
}

func (this *EventHandler) emit(event interface{}, args ...interface{}) {
	this.mtx.Lock()
	slot, ok := this.slots[event]
	if ok {
		slot.emit(args...)
	}
	this.mtx.Unlock()
}

//触发事件
func (this *EventHandler) Emit(event interface{}, args ...interface{}) {
	if this.processQueue != nil {
		this.processQueue.PostNoWait(func() {
			this.emit(event, args...)
		})
	} else {
		this.emit(event, args...)
	}
}

func (this *handlerSlot) register(h *handle) {
	h.slot = this
	this.l.tail.pprev.nnext = h
	h.nnext = &this.l.tail
	h.pprev = this.l.tail.pprev
	this.l.tail.pprev = h
	if this.emiting {
		//在emit回调中注册新的handler不会在本次emit中执行
		h.status = status_register
	}
}

func (this *handlerSlot) pcall(fn interface{}, h Handle, args []interface{}) {
	defer util.Recover(kendynet.GetLogger())
	switch fn.(type) {
	case func():
		fn.(func())()
	case func(Handle):
		fn.(func(Handle))(h)
		break
	case func(...interface{}):
		fn.(func(...interface{}))(args...)
		break
	case func(Handle, ...interface{}):
		fn.(func(Handle, ...interface{}))(h, args...)
		break
	default:
		panic("invaild fn type:" + reflect.TypeOf(fn).Name())
	}
}

func (this *handlerSlot) emit(args ...interface{}) {
	this.emiting = true
	//第一遍遍历,执行事件回调
	cur := this.l.head.nnext
	for cur != &this.l.tail {
		next := cur.nnext
		if cur.status == status_ok {
			this.eh.mtx.Unlock()
			this.pcall(cur.fn, cur.h, args)
			this.eh.mtx.Lock()
		}
		cur = next
	}
	this.emiting = false

	//第二遍遍历执行清除以及status_register->status_ok
	cur = this.l.head.nnext
	for cur != &this.l.tail {
		next := cur.nnext
		if cur.status == status_remove || cur.status == status_ok && cur.once {
			this.remove(cur)
		} else if cur.status == status_register {
			cur.status = status_ok
		}
		cur = next
	}
}

func (this *handlerSlot) clear() {
	cur := this.l.head.nnext
	for cur != &this.l.tail {
		next := cur.nnext
		if this.emiting {
			cur.status = status_remove
		} else {
			this.remove(cur)
		}
		cur = next
	}
}

func (this *handlerSlot) remove(h *handle) {
	delete(this.eh.hmap, h.h)
	h.pprev.nnext = h.nnext
	h.nnext.pprev = h.pprev
	h.slot = nil
	put(h)
}
