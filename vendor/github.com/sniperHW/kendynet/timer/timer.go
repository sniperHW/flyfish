package timer

import (
	"github.com/sniperHW/kendynet"
	"github.com/sniperHW/kendynet/util"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

var (
	globalMgr *TimerMgr = NewTimerMgr(runtime.NumCPU() * 4)
)

const (
	waitting int32 = 0
	firing   int32 = 1
	removed  int32 = 2
)

type Timer struct {
	duration time.Duration
	repeat   bool //是否重复定时器
	status   int32
	callback func(*Timer, interface{})
	p        *p
	ctx      interface{}
	index    *uint64
	t        atomic.Value
}

type p struct {
	sync.Mutex
	index2Timer map[uint64]*Timer
}

func (this *Timer) GetCTX() interface{} {
	return this.ctx
}

func (this *Timer) call() {
	if atomic.CompareAndSwapInt32(&this.status, waitting, firing) {
		if _, err := util.ProtectCall(this.callback, this, this.ctx); nil != err {
			if logger := kendynet.GetLogger(); nil != logger {
				logger.Error("error on timer:", err.Error())
			}
		}

		if this.repeat {
			this.p.resetTicker(this)
		} else {
			atomic.StoreInt32(&this.status, removed)
			if this.index != nil {
				this.p.Lock()
				delete(this.p.index2Timer, *this.index)
				this.p.Unlock()
			}
		}
	}
}

func newp() *p {
	mgr := &p{
		index2Timer: map[uint64]*Timer{},
	}
	return mgr
}

/*
 *  timeout:    超时时间
 *  repeat:     是否重复定时器
 *  eventQue:   如果非nil,callback会被投递到eventQue，否则在定时器主循环中执行
 */

func (this *p) newTimer(timeout time.Duration, repeat bool, fn func(*Timer, interface{}), ctx interface{}, index *uint64) *Timer {
	if nil != fn {
		t := &Timer{
			duration: timeout,
			repeat:   repeat,
			callback: fn,
			p:        this,
			ctx:      ctx,
			index:    index,
		}
		if this.addTimer(t) {
			return t
		} else {
			return nil
		}
	} else {
		return nil
	}
}

func (this *p) addTimer(t *Timer) bool {
	if t.index != nil {
		this.Lock()
		defer this.Unlock()
		if _, ok := this.index2Timer[*t.index]; ok {
			return false
		} else {
			this.index2Timer[*t.index] = t
			t.t.Store(time.AfterFunc(t.duration, func() {
				t.call()
			}))
		}
	} else {
		t.t.Store(time.AfterFunc(t.duration, func() {
			t.call()
		}))
	}
	return true
}

func (this *p) GetTimerByIndex(index uint64) *Timer {
	this.Lock()
	defer this.Unlock()
	if t, ok := this.index2Timer[index]; ok {
		return t
	} else {
		return nil
	}
}

func (this *p) resetTicker(t *Timer) {
	if atomic.CompareAndSwapInt32(&t.status, firing, waitting) {
		//1
		t.t.Store(time.AfterFunc(t.duration, func() {
			t.call()
		}))
		/*
		 * 执行到1的时候,其它线程可能会调用remove,新的定时器还没被设置，因此在remove中Stop的是旧的定时器
		 * 因此这里需要再次判断是否执行了removed,如果是则将前面设置的定时器Stop
		 */
		if atomic.LoadInt32(&t.status) == removed {
			t.t.Load().(*time.Timer).Stop()
		}
	}
}

func (this *p) resetFireTime(t *Timer, timeout time.Duration) bool {
	if t.repeat || atomic.LoadInt32(&t.status) != waitting {
		return false
	}
	return t.t.Load().(*time.Timer).Reset(timeout)
}

func (this *p) remove(t *Timer) bool {
	if atomic.CompareAndSwapInt32(&t.status, waitting, removed) {
		t.t.Load().(*time.Timer).Stop()
		if nil != t.index {
			this.Lock()
			delete(this.index2Timer, *t.index)
			this.Unlock()
		}
		return true
	} else {
		atomic.StoreInt32(&t.status, removed)
		return false
	}
}

func (this *p) removeByIndex(index uint64) (bool, interface{}) {
	this.Lock()
	defer this.Unlock()
	t, ok := this.index2Timer[index]
	if ok {
		if atomic.CompareAndSwapInt32(&t.status, waitting, removed) {
			t.t.Load().(*time.Timer).Stop()
			delete(this.index2Timer, *t.index)
			return true, t.ctx
		} else {
			atomic.StoreInt32(&t.status, removed)
			return false, t.ctx
		}
	} else {
		return false, nil
	}
}

type TimerMgr struct {
	slots []*p
}

func NewTimerMgr(num int) *TimerMgr {

	m := &TimerMgr{
		slots: make([]*p, num),
	}

	for i, _ := range m.slots {
		m.slots[i] = newp()
	}

	return m
}

//一次性定时器
func (this *TimerMgr) Once(timeout time.Duration, callback func(*Timer, interface{}), ctx interface{}) *Timer {
	return this.slots[0].newTimer(timeout, false, callback, ctx, nil)
}

func (this *TimerMgr) OnceWithIndex(timeout time.Duration, callback func(*Timer, interface{}), ctx interface{}, index uint64) *Timer {
	slot := int(index) % len(this.slots)
	return this.slots[slot].newTimer(timeout, false, callback, ctx, &index)
}

//重复定时器
func (this *TimerMgr) Repeat(duration time.Duration, callback func(*Timer, interface{}), ctx interface{}) *Timer {
	return this.slots[0].newTimer(duration, true, callback, ctx, nil)
}

func (this *TimerMgr) GetTimerByIndex(index uint64) *Timer {
	slot := int(index) % len(this.slots)
	return this.slots[slot].GetTimerByIndex(index)
}

func (this *TimerMgr) CancelByIndex(index uint64) (bool, interface{}) {
	slot := int(index) % len(this.slots)
	return this.slots[slot].removeByIndex(index)
}

/*
 *  终止定时器
 *  注意：因为定时器在单独go程序中调度，Cancel不保证能终止定时器的下次执行（例如定时器马上将要被调度执行，此时在另外
 *        一个go程中调用Cancel），对于重复定时器，可以保证定时器最多在执行一次之后终止。
 */
func (this *Timer) Cancel() bool {
	return this.p.remove(this)
}

//只对一次性定时器有效
func (this *Timer) ResetFireTime(timeout time.Duration) bool {
	return this.p.resetFireTime(this, timeout)
}

//一次性定时器
func Once(timeout time.Duration, callback func(*Timer, interface{}), ctx interface{}) *Timer {
	return globalMgr.Once(timeout, callback, ctx)
}

//重复定时器
func Repeat(duration time.Duration, callback func(*Timer, interface{}), ctx interface{}) *Timer {
	return globalMgr.Repeat(duration, callback, ctx)
}

func OnceWithIndex(timeout time.Duration, callback func(*Timer, interface{}), ctx interface{}, index uint64) *Timer {
	return globalMgr.OnceWithIndex(timeout, callback, ctx, index)
}

func GetTimerByIndex(index uint64) *Timer {
	return globalMgr.GetTimerByIndex(index)
}

func CancelByIndex(index uint64) (bool, interface{}) {
	return globalMgr.CancelByIndex(index)
}
