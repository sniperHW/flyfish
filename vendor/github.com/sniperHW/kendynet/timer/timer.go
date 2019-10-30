package timer

import (
	//"fmt"
	"github.com/sniperHW/kendynet"
	"github.com/sniperHW/kendynet/event"
	"github.com/sniperHW/kendynet/util"
	"sync"
	"sync/atomic"
	"time"
)

var once sync.Once

type Timer struct {
	id int64
}

type timer struct {
	heapIdx  uint32
	expired  time.Time //到期时间
	eventQue *event.EventQueue
	timeout  time.Duration
	repeat   bool //是否重复定时器
	callback func(*Timer)
	t        *Timer
	id       int64
}

func (this *timer) Less(o util.HeapElement) bool {
	return o.(*timer).expired.After(this.expired)
}

func (this *timer) GetIndex() uint32 {
	return this.heapIdx
}

func (this *timer) SetIndex(idx uint32) {
	this.heapIdx = idx
}

var (
	idcounter  int64
	notiChan   *util.Notifyer
	minheap    *util.MinHeap
	mtx        sync.Mutex
	idTimerMap map[int64]*timer
)

func pcall(callback func(*Timer), t *Timer) {
	defer util.Recover(kendynet.GetLogger())
	callback(t)
}

func loop() {
	defaultSleepTime := 10 * time.Second
	var tt *time.Timer
	var min util.HeapElement
	for {
		now := time.Now()
		for {
			mtx.Lock()
			min = minheap.Min()
			if nil != min && now.After(min.(*timer).expired) {
				t := min.(*timer)
				minheap.PopMin()
				if !t.repeat {
					delete(idTimerMap, t.id)
				} else {
					t.expired = now.Add(t.timeout)
					minheap.Insert(t)
				}
				mtx.Unlock()
				if nil == t.eventQue {
					pcall(t.callback, t.t)
				} else {
					t.eventQue.PostNoWait(func() {
						pcall(t.callback, t.t)
					})
				}
			} else {
				mtx.Unlock()
				break
			}
		}

		sleepTime := defaultSleepTime
		if nil != min {
			sleepTime = min.(*timer).expired.Sub(now)
		}
		if nil != tt {
			tt.Reset(sleepTime)
		} else {
			tt = time.AfterFunc(sleepTime, func() {
				notiChan.Notify()
			})
		}

		notiChan.Wait()
		tt.Stop()
	}
}

/*
*  timeout:    超时时间
*  repeat:     是否重复定时器
*  eventQue:   如果非nil,callback会被投递到eventQue，否则在定时器主循环中执行
*  返回定时器ID,后面要取消定时器时需要使用这个ID
 */

func newTimer(timeout time.Duration, repeat bool, eventQue *event.EventQueue, fn func(*Timer)) *Timer {

	once.Do(func() {
		notiChan = util.NewNotifyer()
		minheap = util.NewMinHeap(65536)
		idTimerMap = map[int64]*timer{}
		go loop()

	})

	if nil == fn {
		panic("fn == nil")
	}

	id := atomic.AddInt64(&idcounter, 1)

	t := &timer{
		id:       id,
		timeout:  timeout,
		expired:  time.Now().Add(timeout),
		repeat:   repeat,
		callback: fn,
		eventQue: eventQue,
		t: &Timer{
			id: id,
		},
	}

	needNotify := false

	mtx.Lock()
	idTimerMap[t.id] = t
	minheap.Insert(t)
	min := minheap.Min().(*timer)
	if min == t || min.expired.After(t.expired) {
		needNotify = true
	}
	mtx.Unlock()
	if needNotify {
		notiChan.Notify()
	}
	return t.t
}

//一次性定时器
func Once(timeout time.Duration, eventQue *event.EventQueue, callback func(*Timer)) *Timer {
	return newTimer(timeout, false, eventQue, callback)
}

//重复定时器
func Repeat(timeout time.Duration, eventQue *event.EventQueue, callback func(*Timer)) *Timer {
	return newTimer(timeout, true, eventQue, callback)
}

/*
 *  终止定时器
 *  注意：因为定时器在单独go程序中调度，Cancel不保证能终止定时器的下次执行（例如定时器马上将要被调度执行，此时在另外
 *        一个go程中调用Cancel），对于重复定时器，可以保证定时器最多在执行一次之后终止。
 */
func (this *Timer) Cancel() {
	defer mtx.Unlock()
	mtx.Lock()
	id := this.id
	t, ok := idTimerMap[id]
	if ok {
		delete(idTimerMap, id)
		minheap.Remove(t)
	}
}
