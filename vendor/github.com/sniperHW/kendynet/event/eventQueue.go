package event

import (
	"github.com/sniperHW/kendynet"
	"github.com/sniperHW/kendynet/util"
	"sync/atomic"
)

type element struct {
	args []interface{}
	fn   interface{}
}

type EventQueue struct {
	eventQueue *PriorityQueue
	started    int32
}

func NewEventQueueWithName(name string, fullSize ...int) *EventQueue {
	r := &EventQueue{}
	r.eventQueue = NewPriorityQueueWithName(name, fullSize...)
	return r
}

func NewEventQueue(fullSize ...int) *EventQueue {
	r := &EventQueue{}
	r.eventQueue = NewPriorityQueue(fullSize...)
	return r
}

func (this *EventQueue) preparePost(fn interface{}, args ...interface{}) *element {
	return &element{
		fn:   fn,
		args: args,
	}
}

func (this *EventQueue) PostFullReturn(priority int, fn interface{}, args ...interface{}) error {
	return this.eventQueue.AddNoWait(priority, this.preparePost(fn, args...), true)
}

func (this *EventQueue) PostNoWait(priority int, fn interface{}, args ...interface{}) error {
	return this.eventQueue.AddNoWait(priority, this.preparePost(fn, args...))
}

func (this *EventQueue) Post(priority int, fn interface{}, args ...interface{}) error {
	return this.eventQueue.Add(priority, this.preparePost(fn, args...))
}

func (this *EventQueue) Close() {
	this.eventQueue.Close()
}

func (this *EventQueue) Run() {
	if atomic.CompareAndSwapInt32(&this.started, 0, 1) {
		for {
			closed, v := this.eventQueue.Get()
			if nil != v {
				e := v.(*element)
				if _, err := util.ProtectCall(e.fn, e.args...); err != nil {
					logger := kendynet.GetLogger()
					if logger != nil {
						logger.Errorln(err)
					}
				}
			} else if closed {
				return
			}
		}
	}
}
