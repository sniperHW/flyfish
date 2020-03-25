package timer

import (
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

func (this *TimerMgr) setTimer(t *Timer) {
	t.expired = time.Now().Add(t.timeout)
	this.Lock()
	if t.index > 0 {
		this.index2Timer[t.index] = t
	}
	this.minheap.Insert(t)
	min := this.minheap.Min().(*Timer)
	if min == t || min.expired.After(t.expired) {
		this.notiChan.Notify()
	}
	this.Unlock()
}

func (this *TimerMgr) GetTimerByIndex(index uint64) *Timer {
	this.Lock()
	if t, ok := this.index2Timer[index]; ok {
		this.Unlock()
		return t
	} else {
		this.Unlock()
		return nil
	}
}

func (this *TimerMgr) resetFireTime(t *Timer, timeout time.Duration) {
	t.timeout = timeout
	t.expired = time.Now().Add(t.timeout)
	this.Lock()
	this.minheap.Fix(t)
	min := this.minheap.Min().(*Timer)
	if min == t || min.expired.After(t.expired) {
		this.notiChan.Notify()
	}
	this.Unlock()
}

func (this *TimerMgr) resetDuration(t *Timer, duration time.Duration) {
	t.timeout = duration
	t.expired = time.Now().Add(t.timeout)
	if t.firing {
		return
	} else {
		this.Lock()
		this.minheap.Fix(t)
		min := this.minheap.Min().(*Timer)
		if min == t || min.expired.After(t.expired) {
			this.notiChan.Notify()
		}
		this.Unlock()
	}
}

func (this *TimerMgr) loop() {
	defaultSleepTime := 10 * time.Second
	var tt *time.Timer
	var min util.HeapElement
	for {
		now := time.Now()
		for {
			this.Lock()
			min = this.minheap.Min()
			if nil != min && now.After(min.(*Timer).expired) {
				t := min.(*Timer)
				this.minheap.PopMin()
				if t.index > 0 {
					delete(this.index2Timer, t.index)
				}
				this.Unlock()
				t.call()
			} else {
				this.Unlock()
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
		this.setTimer(t)
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

func (this *TimerMgr) remove(t *Timer) {
	this.Lock()
	defer this.Unlock()
	if t.index > 0 {
		delete(this.index2Timer, t.index)
	}
	this.minheap.Remove(t)
}

type Timer struct {
	sync.Mutex
	heapIdx  int
	expired  time.Time //到期时间
	eventQue *event.EventQueue
	timeout  time.Duration
	repeat   bool //是否重复定时器
	firing   bool
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

func pcall(callback func(*Timer, interface{}), t *Timer) {
	defer util.Recover(kendynet.GetLogger())
	callback(t, t.ctx)
}

func (this *Timer) preCall() bool {
	this.Lock()
	if this.canceled {
		this.Unlock()
		return false
	} else {
		this.firing = true
		this.Unlock()
		return true
	}
}

func (this *Timer) call_() {

	this.Lock()
	if this.canceled {
		this.Unlock()
		return
	} else {
		this.firing = true
		this.Unlock()
	}

	pcall(this.callback, this)
	if this.repeat {
		this.Lock()
		defer this.Unlock()
		if !this.canceled {
			this.firing = false
			this.mgr.setTimer(this)
		}
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
	this.Lock()
	defer this.Unlock()
	if this.canceled {
		return false
	}
	this.canceled = true
	if !this.firing {
		this.mgr.remove(this)
	}
	return this.firing == false
}

//只对一次性定时器有效
func (this *Timer) ResetFireTime(timeout time.Duration) bool {
	this.Lock()
	defer this.Unlock()
	if this.canceled {
		return false
	}

	if this.repeat {
		return false
	}

	if !this.firing {
		this.mgr.resetFireTime(this, timeout)
	}

	return this.firing == false
}

//只对重复定时器有效
func (this *Timer) ResetDuration(duration time.Duration) bool {
	this.Lock()
	defer this.Unlock()
	if this.canceled {
		return false
	}

	if !this.repeat {
		return false
	}

	this.mgr.resetDuration(this, duration)
	return true
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
