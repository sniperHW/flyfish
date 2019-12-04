package timer

import (
	"github.com/sniperHW/kendynet"
	"github.com/sniperHW/kendynet/event"
	"github.com/sniperHW/kendynet/util"
	"sync"
	"sync/atomic"
	"time"
)

var (
	once      sync.Once
	globalMgr *TimerMgr
)

type TimerMgr struct {
	sync.Mutex
	notiChan *util.Notifyer
	minheap  *util.MinHeap
}

func NewTimerMgr() *TimerMgr {
	mgr := &TimerMgr{
		notiChan: util.NewNotifyer(),
		minheap:  util.NewMinHeap(65536),
	}
	go mgr.loop()
	return mgr
}

func (this *TimerMgr) setTimer(t *Timer, inloop bool) {
	needNotify := false
	t.expired = time.Now().Add(t.timeout)
	this.Lock()
	this.minheap.Insert(t)
	min := this.minheap.Min().(*Timer)
	if min == t || min.expired.After(t.expired) {
		needNotify = true
	}
	this.Unlock()
	if !inloop && needNotify {
		this.notiChan.Notify()
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

func (this *TimerMgr) newTimer(timeout time.Duration, repeat bool, eventQue *event.EventQueue, fn func(*Timer)) *Timer {
	if nil == fn {
		return nil
	}

	t := &Timer{
		timeout:  timeout,
		repeat:   repeat,
		callback: fn,
		eventQue: eventQue,
		mgr:      this,
	}

	this.setTimer(t, false)

	return t
}

//一次性定时器
func (this *TimerMgr) Once(timeout time.Duration, eventQue *event.EventQueue, callback func(*Timer)) *Timer {
	return this.newTimer(timeout, false, eventQue, callback)
}

//重复定时器
func (this *TimerMgr) Repeat(timeout time.Duration, eventQue *event.EventQueue, callback func(*Timer)) *Timer {
	return this.newTimer(timeout, true, eventQue, callback)
}

func (this *TimerMgr) remove(t *Timer) {
	this.Lock()
	defer this.Unlock()
	this.minheap.Remove(t)
}

type Timer struct {
	sync.Mutex
	heapIdx  uint32
	expired  time.Time //到期时间
	eventQue *event.EventQueue
	timeout  time.Duration
	repeat   bool //是否重复定时器
	firing   int32
	canceled int32
	callback func(*Timer)
	mgr      *TimerMgr
}

func (this *Timer) Less(o util.HeapElement) bool {
	return o.(*Timer).expired.After(this.expired)
}

func (this *Timer) GetIndex() uint32 {
	return this.heapIdx
}

func (this *Timer) SetIndex(idx uint32) {
	this.heapIdx = idx
}

func pcall(callback func(*Timer), t *Timer) {
	defer util.Recover(kendynet.GetLogger())
	callback(t)
}

func (this *Timer) call_(inloop bool) {

	this.Lock()
	if this.canceled == 1 {
		this.Unlock()
		return
	} else {
		this.firing = 1
	}
	this.Unlock()

	pcall(this.callback, this)

	if this.repeat {
		this.Lock()
		if this.canceled == 0 {
			this.firing = 0
			this.mgr.setTimer(this, inloop)
		}
		this.Unlock()
	}
}

func (this *Timer) call() {
	if atomic.LoadInt32(&this.canceled) == 1 {
		return
	} else {
		if nil == this.eventQue {
			this.call_(true)
		} else {
			this.eventQue.PostNoWait(func() {
				this.call_(false)
			})
		}
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
	if this.canceled == 1 {
		return false
	}
	this.canceled = 1
	if this.firing == 0 {
		this.mgr.remove(this)
	}
	return this.firing == 0
}

//一次性定时器
func Once(timeout time.Duration, eventQue *event.EventQueue, callback func(*Timer)) *Timer {
	once.Do(func() {
		globalMgr = NewTimerMgr()
	})
	return globalMgr.Once(timeout, eventQue, callback)
}

//重复定时器
func Repeat(timeout time.Duration, eventQue *event.EventQueue, callback func(*Timer)) *Timer {
	once.Do(func() {
		globalMgr = NewTimerMgr()
	})
	return globalMgr.Repeat(timeout, eventQue, callback)
}
