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
	e := &element{}
	switch fn.(type) {
	case func():
		e.fn = fn
		break
	case func([]interface{}):
		e.fn = fn
		e.args = args
		break
	default:
		panic("invaild callback type")
	}
	return e
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

func pcall(fn interface{}, args []interface{}) {
	defer util.Recover(kendynet.GetLogger())
	switch fn.(type) {
	case func():
		fn.(func())()
		break
	case func([]interface{}):
		fn.(func([]interface{}))(args)
		break
	}
}

func (this *EventQueue) Run() {

	if !atomic.CompareAndSwapInt32(&this.started, 0, 1) {
		return
	}

	for {
		closed, localList := this.eventQueue.Get()
		for _, v := range localList {
			e := v.(*element)
			pcall(e.fn, e.args)
		}
		if closed {
			return
		}
	}
}
