package movingAverage

import (
	"sync"
)

//移动平均
type MovingAverage struct {
	sync.Mutex
	head    int
	window  []int
	total   int
	average int
	wc      int
}

func NewMovingAverage(window int) *MovingAverage {
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
	ma.average = ma.total / ma.wc
}

func (ma MovingAverage) GetAverage() int {
	ma.Lock()
	defer ma.Unlock()
	return ma.average
}
