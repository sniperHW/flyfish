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
	eventQueue *util.BlockQueue
	started    int32
}

func NewEventQueueWithName(name string, fullSize ...int) *EventQueue {
	r := &EventQueue{}
	r.eventQueue = util.NewBlockQueueWithName(name, fullSize...)
	return r
}

func NewEventQueue(fullSize ...int) *EventQueue {
	r := &EventQueue{}
	r.eventQueue = util.NewBlockQueue(fullSize...)
	return r
}

func (this *EventQueue) preparePost(fn interface{}, args ...interface{}) *element {
	return &element{
		fn:   fn,
		args: args,
	}
}

func (this *EventQueue) PostFullReturn(fn interface{}, args ...interface{}) error {
	return this.eventQueue.AddNoWait(this.preparePost(fn, args...), true)
}

func (this *EventQueue) PostNoWait(fn interface{}, args ...interface{}) error {
	return this.eventQueue.AddNoWait(this.preparePost(fn, args...))
}

func (this *EventQueue) Post(fn interface{}, args ...interface{}) error {
	return this.eventQueue.Add(this.preparePost(fn, args...))
}

func (this *EventQueue) Close() {
	this.eventQueue.Close()
}

func (this *EventQueue) Run() {
	if atomic.CompareAndSwapInt32(&this.started, 0, 1) {
		for {
			closed, localList := this.eventQueue.Get()
			for _, v := range localList {
				e := v.(*element)
				if _, err := util.ProtectCall(e.fn, e.args...); err != nil {
					logger := kendynet.GetLogger()
					if logger != nil {
						logger.Errorln(err)
					}
				}
			}
			if closed {
				return
			}
		}
	}
}
