package movingAverage

import (
	"sync"
	"sync/atomic"
)

//移动平均
type MovingAverage struct {
	sync.Mutex
	head    int
	window  []int
	total   int
	wc      int
	average int64
}

func New(window int) *MovingAverage {
	return &MovingAverage{
		window: make([]int, window),
	}
}

func (ma *MovingAverage) Add(v int) {
	ma.Lock()
	defer ma.Unlock()
	if ma.wc < len(ma.window) {
		//窗口没有填满
		ma.window[ma.head] = v
		ma.head = (ma.head + 1) % len(ma.window)
		ma.wc++
	} else {
		ma.total -= ma.window[ma.head]
		ma.window[ma.head] = v
		ma.head = (ma.head + 1) % len(ma.window)
	}
	ma.total += v
	atomic.StoreInt64(&ma.average, int64(ma.total/ma.wc))
}

func (ma *MovingAverage) GetAverage() int {
	return int(atomic.LoadInt64(&ma.average))
}
