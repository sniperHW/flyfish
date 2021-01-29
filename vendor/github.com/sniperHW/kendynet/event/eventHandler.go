package event

import (
	"github.com/sniperHW/kendynet"
	"github.com/sniperHW/kendynet/util"
	"reflect"
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
	version int64
}

const (
	opEmit     = 1
	opDelete   = 2
	opRegister = 3
)

type op struct {
	ppnext *op
	opType int
	args   []interface{}
	h      *handle
}

type handlerSlot struct {
	sync.Mutex
	l         handList
	emiting   bool
	current   int
	version   int64
	pendingOP *op
}

type EventHandler struct {
	sync.RWMutex
	slots   map[interface{}]*handlerSlot
	version int64
}

func NewEventHandler() *EventHandler {
	return &EventHandler{
		slots: map[interface{}]*handlerSlot{},
	}
}

func (this *EventHandler) register(event interface{}, once bool, fn interface{}) Handle {
	if nil == event {
		panic("event == nil")
	}

	if reflect.TypeOf(fn).Kind() != reflect.Func {
		panic("fn should be func type")

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
			version: atomic.AddInt64(&this.version, 1),
		}

		slot.l.head.nnext = &slot.l.tail
		slot.l.tail.pprev = &slot.l.head

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
	if ok && hh.version == slot.version {
		slot.remove(h)
	}
}

//触发事件
func (this *EventHandler) Emit(event interface{}, args ...interface{}) {
	this.RLock()
	slot, ok := this.slots[event]
	this.RUnlock()
	if ok {
		slot.emit(args...)
	}
}

/*
 * 通过事件队列来执行emit
 */

type EventQueueParam struct {
	Priority   int
	Q          *EventQueue
	BlockMode  bool //阻塞模式，如果Q满阻塞调用方
	FullReturn bool //BlockMode==false时,如果FullReturn==true,则当Q满时返回ErrQueueFull,否则不管Q是否满都将emit投递过去
}

func (this *EventHandler) EmitToEventQueue(param EventQueueParam, event interface{}, args ...interface{}) error {
	a := make([]interface{}, 0, len(args)+1)
	a = append(a, event)
	a = append(a, args...)
	if param.BlockMode {
		return param.Q.Post(param.Priority, this.Emit, a...)
	} else if param.FullReturn {
		return param.Q.PostFullReturn(param.Priority, this.Emit, a...)
	} else {
		return param.Q.PostNoWait(param.Priority, this.Emit, a...)
	}
}

func (this *EventHandler) Clear(event interface{}) {
	this.Lock()
	defer this.Unlock()
	delete(this.slots, event)
}

func (this *handlerSlot) push(item *op) {
	var head *op
	if this.pendingOP == nil {
		head = item
	} else {
		head = this.pendingOP.ppnext
		this.pendingOP.ppnext = item
	}
	item.ppnext = head
	this.pendingOP = item
}

func (this *handlerSlot) pop() *op {
	if this.pendingOP == nil {
		return nil
	} else {
		item := this.pendingOP.ppnext
		if item == this.pendingOP {
			this.pendingOP = nil
		} else {
			this.pendingOP.ppnext = item.ppnext
		}

		item.ppnext = nil
		return item
	}
}

func (this *handlerSlot) doRegister(h *handle) {
	h.version = this.version
	this.l.tail.pprev.nnext = h
	h.nnext = &this.l.tail
	h.pprev = this.l.tail.pprev
	this.l.tail.pprev = h
}

func (this *handlerSlot) register(h *handle) {
	this.Lock()
	defer this.Unlock()
	if this.emiting {
		this.push(&op{
			opType: opRegister,
			h:      h,
		})
	} else {
		this.doRegister(h)
	}
}

func (this *handlerSlot) doRemove(h *handle) {
	if h.version == this.version {
		h.pprev.nnext = h.nnext
		h.nnext.pprev = h.pprev
		h.nnext = nil
		h.pprev = nil
		h.version = 0
	}
}

func (this *handlerSlot) remove(h *handle) {
	this.Lock()
	defer this.Unlock()
	if this.emiting {
		this.push(&op{
			opType: opDelete,
			h:      h,
		})
	} else {
		this.doRemove(h)
	}
}

func pcall2(h *handle, args []interface{}) {

	var arguments []interface{}

	fnType := reflect.TypeOf(h.fn)

	if fnType.NumIn() > 0 && fnType.In(0) == reflect.TypeOf((Handle)(h)) {
		arguments = make([]interface{}, len(args)+1, len(args)+1)
		arguments[0] = h
		copy(arguments[1:], args)
	} else {
		arguments = args
	}

	if _, err := util.ProtectCall(h.fn, arguments...); err != nil {
		logger := kendynet.GetLogger()
		if logger != nil {
			logger.Errorln(err)
		}
	}
}

func (this *handlerSlot) emit(args ...interface{}) {
	this.Lock()
	this.push(&op{
		opType: opEmit,
		args:   args,
	})

	if this.emiting {
		this.Unlock()
	} else {
		this.emiting = true
		/*
		 *  在执行doEmit的时候，其它go程或事件处理器可能会往pendingOP中添加请求
		 *  因此当前的执行者必须把这些pendingOP全部执行完毕。
		 */
		for {
			if o := this.pop(); nil == o {
				break
			} else {
				switch o.opType {
				case opDelete:
					this.doRemove(o.h)
				case opRegister:
					this.doRegister(o.h)
				case opEmit:
					cur := this.l.head.nnext
					for cur != &this.l.tail {
						this.Unlock()
						pcall2(cur, o.args)
						this.Lock()
						next := cur.nnext
						if cur.once {
							this.doRemove(cur)
						}
						cur = next
					}
				}
			}
		}
		this.emiting = false
		this.Unlock()
	}
}
