package event

import (
	"github.com/sniperHW/kendynet"
	"github.com/sniperHW/kendynet/util"
	"sync/atomic"
)

/*
 *    支持优先级的事件队列
 *    考虑一个基于事件队列的异步服务模型，从网络接收请求，投递到消息队列执行，请求可能涉及到外部服务调用，外部调用的结果也通过投递到消息队列处理，然后返回给请求方。
 *
 *      db <------------------
 *      |                    |
 *      | db op resp         |  db op
 *      |                    |
 *      |           request---
 *      |         /
 *     eventqueue
 *         |      \
 *		   |	    db op resp -> process -- send response---
 *         |                                                 |
 *         |                                                 |
 *         |                                                 |
 *		 client <--------------------------------------------
 *
 *    在此模型下，如果消息队列只有唯一的优先级，则db返回将在被排队的request后面处理，导致响应延迟
 *
 *    因此，可以使用2个优先级的队列，db操作结果为高优先级，客户端请求为低优先级。
 *
 */

type element struct {
	args []interface{}
	fn   interface{}
}

type EventQueue struct {
	eventQueue *PriorityQueue
	started    int32
}

func NewEventQueueWithPriority(priority int, fullSize ...int) *EventQueue {
	r := &EventQueue{}
	r.eventQueue = NewPriorityQueue(priority, fullSize...)
	return r
}

func NewEventQueue(fullSize ...int) *EventQueue {
	r := &EventQueue{}
	r.eventQueue = NewPriorityQueue(1, fullSize...)
	return r
}

func (this *EventQueue) preparePost(fn interface{}, args ...interface{}) *element {
	return &element{
		fn:   fn,
		args: args,
	}
}

//投递闭包，如果队列满返回ErrQueueFull
func (this *EventQueue) PostFullReturn(priority int, fn interface{}, args ...interface{}) error {
	return this.eventQueue.AddNoWait(priority, this.preparePost(fn, args...), true)
}

//投递闭包，忽略容量限制
func (this *EventQueue) PostNoWait(priority int, fn interface{}, args ...interface{}) error {
	return this.eventQueue.AddNoWait(priority, this.preparePost(fn, args...))
}

//投递闭包，如果队列满将阻塞
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
