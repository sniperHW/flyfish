package event

import (
	"fmt"
	"github.com/sniperHW/kendynet/util"
	"github.com/sniperHW/kendynet"
	"sync/atomic"
	"runtime"
)


type element struct {
	tt            int
	args          []interface{}
	callback      interface{}
}

type EventQueue struct {
	eventQueue *util.BlockQueue
	started    int32	
}

func NewEventQueueWithName(name string,fullSize ...int) *EventQueue {
	r := &EventQueue{}
	r.eventQueue = util.NewBlockQueueWithName(name,fullSize...)
	return r
}

func NewEventQueue(fullSize ...int) *EventQueue {
	r := &EventQueue{}
	r.eventQueue = util.NewBlockQueue(fullSize...)
	return r
}

func (this *EventQueue) Post(callback interface{},args ...interface{}) {
	
	e := element{}

	switch callback.(type) {
	case func():
		e.tt = tt_noargs
		e.callback = callback
		break
	case func([]interface{}):
		e.tt = tt_varargs
		e.callback = callback
		e.args = args
		break
	default:
		panic("invaild callback type")
	}
	this.eventQueue.Add(&e)
}

func (this *EventQueue) Close() {
	this.eventQueue.Close()
}


func pcall(tt int,callback interface{},args []interface{}) {
	defer func(){
		if r := recover(); r != nil {
			buf := make([]byte, 65535)
			l := runtime.Stack(buf, false)
			kendynet.Errorf("%v: %s\n", r, buf[:l])
		}			
	}()	

	if tt == tt_noargs {
		callback.(func())()
	} else {
		callback.(func([]interface{}))(args)
	}	
}

func (this *EventQueue) Run() error {

	if !atomic.CompareAndSwapInt32(&this.started, 0, 1) {
		return fmt.Errorf("already started")
	}

	for {
		closed, localList := this.eventQueue.Get()
		for _,v := range(localList) {
			e := v.(*element)
			pcall(e.tt,e.callback,e.args)
		}
		if closed {
			return nil
		}
	}
}