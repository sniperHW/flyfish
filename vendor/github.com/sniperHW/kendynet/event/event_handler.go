package event

import(
	"sync"
	"fmt"
)

const (
	tt_noargs  = 1   //无参回调
	tt_varargs = 2   //不定参数回调
)

const (
	status_register  = 1
	status_watting   = 2
	status_emitting  = 3
	status_remove    = 4
)

type Handle struct {
	tt            int
	callback      interface{}
	once          bool
	prev         *Handle
	next         *Handle
	mtx      	  sync.Mutex
	status        int
	event         interface{}
	slot         *handlerSlot
}

type handlerSlot struct {
	head     	Handle
	tail     	Handle
}

type EventHandler struct {
	slots map[interface{}]*handlerSlot
	processQueue *EventQueue
}


func NewEventHandler(processQueue *EventQueue) *EventHandler {
	if nil == processQueue {
		return nil
	} else {
		return &EventHandler{
			slots : map[interface{}]*handlerSlot{},
			processQueue : processQueue,
		}
	}
}


func (this *EventHandler) Register(event interface{},once bool,callback interface{}) (*Handle,error) {

	if nil == event {
		return nil,fmt.Errorf("event == nil")
	}

	var tt int

	switch callback.(type) {
	case func():
		tt = tt_noargs
		break
	case func([]interface{}):
		tt = tt_varargs
		break
	default:
		return nil,fmt.Errorf("invaild callback type")
	}
	
	handle := &Handle{
		tt       : tt,
		callback : callback,
		once     : once,
		status   : status_register,
		event    : event,
	}

	this.processQueue.PostNoWait(func() {
		slot,ok := this.slots[event]
		if !ok {
			slot = &handlerSlot{
			}
			slot.head.next = &slot.tail
			slot.tail.prev = &slot.head
			this.slots[event] = slot 
		}
		slot.register(handle)		
	})
	return handle,nil
}

func (this *EventHandler) Remove(handle *Handle) bool {
	defer handle.mtx.Unlock()
	handle.mtx.Lock()
	if handle.status == status_emitting {
		/* 正在触发，设置once标记
		*  当回调执行完毕回来发现once标记后执行删除
		*/
		handle.once = true
	} else {
		if handle.status != status_watting {
			return false
		}
		handle.status = status_remove
		this.processQueue.PostNoWait(func() {
			slot,ok := this.slots[handle.event]
			if ok {
				slot.remove(handle)
			}
		})
	}
	return true
}

func (this *EventHandler) Clear(event interface{}) {
	this.processQueue.PostNoWait(func() {
		slot,ok := this.slots[event]
		if ok {
			slot.clear()
		}
	})
}

//触发事件
func (this *EventHandler) Emit(event interface{},args ...interface{}) {
	this.processQueue.PostNoWait(func() {
		slot,ok := this.slots[event]
		if ok {
			slot.emit(args...)
		}
	})
}


func (this *handlerSlot) register(handle *Handle) {
	
	defer handle.mtx.Unlock()
	handle.mtx.Lock()
	if handle.status == status_remove {
		return
	}

	handle.next = &this.tail
	handle.prev = this.tail.prev

	this.tail.prev.next = handle
	this.tail.prev = handle

	handle.status = status_watting
	handle.slot   = this

}

func (this *handlerSlot) emit(args ...interface{}) {
	cur := this.head.next
	for cur != &this.tail {
		cur.mtx.Lock()
		if cur.status == status_watting {
			cur.status = status_emitting
			cur.mtx.Unlock()
			pcall(cur.tt,cur.callback,args)
			cur.mtx.Lock()
			if cur.once {
				cur.status = status_remove
			} else {
				cur.status = status_watting
			}
			cur.mtx.Unlock()
			if cur.status == status_remove {
				cur = this.remove(cur)
			} else {
				cur = cur.next
			}
		} else {
			cur.status = status_remove
			cur.mtx.Unlock()
			cur = this.remove(cur)
		}
	}
}

func (this *handlerSlot) clear() { 
	cur := this.head.next
	for cur != &this.tail {
		cur.mtx.Lock()

		cur.status = status_remove
		cur.mtx.Unlock()
		cur = this.remove(cur)
	}
}

func (this *handlerSlot) remove(handle *Handle) *Handle {
	if handle.next == nil || handle.prev == nil || handle.slot != this {
		return nil
	}
	next := handle.next

	handle.prev.next = handle.next
	handle.next.prev = handle.prev
	handle.next = nil
	handle.prev = nil
	handle.slot = nil
	return next
}
