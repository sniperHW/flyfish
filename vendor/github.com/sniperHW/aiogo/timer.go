package aiogo

import (
	"container/heap"
	//"fmt"
	"sync"
	"time"
)

type timedHeap []*op

func (h timedHeap) Len() int           { return len(h) }
func (h timedHeap) Less(i, j int) bool { return h[i].deadline.Before(h[j].deadline) }
func (h timedHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].idx = i
	h[j].idx = j
}

func (h *timedHeap) Push(x interface{}) {
	*h = append(*h, x.(*op))
	n := len(*h)
	(*h)[n-1].idx = n - 1
}
func (h *timedHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	old[n-1] = nil // avoid memory leak
	*h = old[0 : n-1]
	return x
}

type timer struct {
	heap      timedHeap
	die       chan struct{}
	timer     *time.Timer
	onTimeout func(*op)
	closeOnce sync.Once
}

func newTimer(w *worker, onTimeout func(*op)) *timer {
	t := &timer{
		die:       make(chan struct{}),
		timer:     time.NewTimer(0),
		onTimeout: onTimeout,
	}

	go func() {
		for {
			select {
			case <-t.die:
				return
			case <-t.timer.C:
				op := gOpPool.get()
				op.tt = timeoutNotify
				if nil != w.push(op) {
					return
				}
			}
		}
	}()

	return t
}

func (t *timer) Close() {
	t.closeOnce.Do(func() {
		close(t.die)
	})
}

func (t *timer) Add(o *op, deadline time.Time) {
	o.deadline = deadline
	o.timer = t
	heap.Push(&t.heap, o)
	if t.heap[0] == o {
		t.timer.Reset(o.deadline.Sub(time.Now()))
	}
}

func (t *timer) Remove(o *op) {
	heap.Remove(&t.heap, o.idx)
}

func (t *timer) tick() {
	for t.heap.Len() > 0 {
		now := time.Now()
		o := t.heap[0]
		if now.After(o.deadline) {
			heap.Pop(&t.heap)
			t.onTimeout(o)
		} else {
			t.timer.Reset(o.deadline.Sub(now))
			break
		}
	}
}
