package goaio

import (
	"sync/atomic"
	"time"
)

const (
	waitting int32 = 0
	firing   int32 = 1
	removed  int32 = 2
)

type Timer struct {
	duration time.Duration
	status   int32
	callback func(*Timer)
	t        atomic.Value
}

func (this *Timer) call() {
	if atomic.CompareAndSwapInt32(&this.status, waitting, firing) {
		this.callback(this)
		atomic.StoreInt32(&this.status, removed)
	}
}

func (this *Timer) Cancel() bool {
	if atomic.CompareAndSwapInt32(&this.status, waitting, removed) {
		this.t.Load().(*time.Timer).Stop()
		return true
	} else {
		atomic.StoreInt32(&this.status, removed)
		return false
	}
}

func newTimer(timeout time.Duration, fn func(*Timer)) *Timer {
	if nil != fn {
		t := &Timer{
			duration: timeout,
			callback: fn,
		}

		t.t.Store(time.AfterFunc(t.duration, func() {
			t.call()
		}))

		return t
	} else {
		return nil
	}
}
