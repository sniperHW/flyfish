package timer

import (
	"fmt"
	"github.com/sniperHW/kendynet"
	"github.com/sniperHW/kendynet/event"
	"github.com/sniperHW/kendynet/util"
	"sync"
	"time"
)

var (
	once      sync.Once
	globalMgr *TimerMgr
)

type TimerMgr struct {
	sync.Mutex
	notiChan    *util.Notifyer
	minheap     util.MinHeap
	index2Timer map[uint64]*Timer
}

func NewTimerMgr() *TimerMgr {
	mgr := &TimerMgr{
		notiChan:    util.NewNotifyer(),
		minheap:     util.NewMinHeap(4096),
		index2Timer: map[uint64]*Timer{},
	}
	go mgr.loop()
	return mgr
}

func (this *TimerMgr) setTimer(t *Timer, index uint64) {
	this.Lock()
	defer this.Unlock()
	if !t.canceled {
		t.expired = time.Now().Add(t.timeout)
		if index > 0 {
			this.index2Timer[index] = t
		}
		this.minheap.Insert(t)
		if t == this.minheap.Min().(*Timer) {
			this.notiChan.Notify()
		}
	}
}

func (this *TimerMgr) GetTimerByIndex(index uint64) *Timer {
	this.Lock()
	defer this.Unlock()
	if t, ok := this.index2Timer[index]; ok {
		return t
	} else {
		return nil
	}
}

func (this *TimerMgr) resetFireTime(t *Timer, timeout time.Duration) bool {
	this.Lock()
	defer this.Unlock()
	if t.canceled || t.GetIndex() == -1 {
		return false
	} else {
		t.timeout = timeout
		t.expired = time.Now().Add(t.timeout)
		this.minheap.Fix(t)
		if t == this.minheap.Min().(*Timer) {
			this.notiChan.Notify()
		}
		return true
	}
}

func (this *TimerMgr) resetDuration(t *Timer, duration time.Duration) bool {
	this.Lock()
	defer this.Unlock()
	if t.canceled {
		return false
	} else {
		t.timeout = duration
		t.expired = time.Now().Add(t.timeout)
		if t.GetIndex() != -1 {
			this.minheap.Fix(t)
			if t == this.minheap.Min().(*Timer) {
				this.notiChan.Notify()
			}
		}
		return true
	}
}

func (this *TimerMgr) loop() {
	defaultSleepTime := 10 * time.Second
	var tt *time.Timer
	var min util.HeapElement
	for {
		now := time.Now()
		this.Lock()
		for {
			min = this.minheap.Min()
			if nil != min && now.After(min.(*Timer).expired) {
				t := min.(*Timer)
				this.minheap.PopMin()
				if !t.repeat && t.index > 0 {
					delete(this.index2Timer, t.index)
				}
				this.Unlock()
				t.call()
				this.Lock()
			} else {
				break
			}
		}

		sleepTime := defaultSleepTime
		if nil != min {
			sleepTime = min.(*Timer).expired.Sub(now)
		}
		if nil != tt {
			tt.Reset(sleepTime)
		} else {
			tt = time.AfterFunc(sleepTime, func() {
				this.notiChan.Notify()
			})
		}
		this.Unlock()

		this.notiChan.Wait()
		tt.Stop()
	}
}

/*
 *  timeout:    超时时间
 *  repeat:     是否重复定时器
 *  eventQue:   如果非nil,callback会被投递到eventQue，否则在定时器主循环中执行
 */

func (this *TimerMgr) newTimer(timeout time.Duration, repeat bool, eventQue *event.EventQueue, fn func(*Timer, interface{}), ctx interface{}, index uint64) *Timer {
	if nil != fn {
		t := &Timer{
			timeout:  timeout,
			repeat:   repeat,
			callback: fn,
			eventQue: eventQue,
			mgr:      this,
			ctx:      ctx,
			index:    index,
		}
		this.setTimer(t, index)
		return t
	} else {
		return nil
	}
}

//一次性定时器
func (this *TimerMgr) Once(timeout time.Duration, eventQue *event.EventQueue, callback func(*Timer, interface{}), ctx interface{}) *Timer {
	return this.newTimer(timeout, false, eventQue, callback, ctx, 0)
}

func (this *TimerMgr) OnceWithIndex(timeout time.Duration, eventQue *event.EventQueue, callback func(*Timer, interface{}), ctx interface{}, index uint64) *Timer {
	if index > 0 {
		return this.newTimer(timeout, false, eventQue, callback, ctx, index)
	} else {
		return nil
	}
}

//重复定时器
func (this *TimerMgr) Repeat(duration time.Duration, eventQue *event.EventQueue, callback func(*Timer, interface{}), ctx interface{}) *Timer {
	return this.newTimer(duration, true, eventQue, callback, ctx, 0)
}

func (this *TimerMgr) remove(t *Timer) bool {
	this.Lock()
	defer this.Unlock()

	if t.canceled {
		return false
	}

	t.canceled = true
	if t.GetIndex() == -1 {
		return false
	} else {
		if t.index > 0 {
			delete(this.index2Timer, t.index)
		}
		this.minheap.Remove(t)
		return true
	}
}

type Timer struct {
	heapIdx  int
	expired  time.Time //到期时间
	eventQue *event.EventQueue
	timeout  time.Duration
	repeat   bool //是否重复定时器
	canceled bool
	callback func(*Timer, interface{})
	mgr      *TimerMgr
	ctx      interface{}
	index    uint64
}

func (this *Timer) Less(o util.HeapElement) bool {
	return o.(*Timer).expired.After(this.expired)
}

func (this *Timer) GetIndex() int {
	return this.heapIdx
}

func (this *Timer) SetIndex(idx int) {
	this.heapIdx = idx
}

func (this *Timer) GetCTX() interface{} {
	return this.ctx
}

func (this *Timer) call_() {

	if _, err := util.ProtectCall(this.callback, this, this.ctx); nil != err {
		logger := kendynet.GetLogger()
		if nil != logger {
			logger.Errorln("error on timer:", err.Error())
		} else {
			fmt.Println("error on timer:", err.Error())
		}
	}

	if this.repeat {
		this.mgr.setTimer(this, 0)
	}
}

func (this *Timer) call() {
	if nil == this.eventQue {
		this.call_()
	} else {
		this.eventQue.PostNoWait(func() {
			this.call_()
		})
	}
}

/*
 *  终止定时器
 *  注意：因为定时器在单独go程序中调度，Cancel不保证能终止定时器的下次执行（例如定时器马上将要被调度执行，此时在另外
 *        一个go程中调用Cancel），对于重复定时器，可以保证定时器最多在执行一次之后终止。
 */
func (this *Timer) Cancel() bool {
	return this.mgr.remove(this)
}

//只对一次性定时器有效
func (this *Timer) ResetFireTime(timeout time.Duration) bool {
	return this.mgr.resetFireTime(this, timeout)
}

//只对重复定时器有效
func (this *Timer) ResetDuration(duration time.Duration) bool {
	return this.mgr.resetDuration(this, duration)
}

//一次性定时器
func Once(timeout time.Duration, eventQue *event.EventQueue, callback func(*Timer, interface{}), ctx interface{}) *Timer {
	once.Do(func() {
		globalMgr = NewTimerMgr()
	})
	return globalMgr.Once(timeout, eventQue, callback, ctx)
}

//重复定时器
func Repeat(duration time.Duration, eventQue *event.EventQueue, callback func(*Timer, interface{}), ctx interface{}) *Timer {
	once.Do(func() {
		globalMgr = NewTimerMgr()
	})
	return globalMgr.Repeat(duration, eventQue, callback, ctx)
}

func OnceWithIndex(timeout time.Duration, eventQue *event.EventQueue, callback func(*Timer, interface{}), ctx interface{}, index uint64) *Timer {
	once.Do(func() {
		globalMgr = NewTimerMgr()
	})
	return globalMgr.OnceWithIndex(timeout, eventQue, callback, ctx, index)
}

func GetTimerByIndex(index uint64) *Timer {
	once.Do(func() {
		globalMgr = NewTimerMgr()
	})
	return globalMgr.GetTimerByIndex(index)
}
