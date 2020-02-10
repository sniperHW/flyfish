package util

import (
	"container/heap"
)

type HeapElement interface {
	//if self less than other return true
	Less(HeapElement) bool
	GetIndex() int
	SetIndex(int)
}

type MinHeap []HeapElement

func (h MinHeap) Len() int           { return len(h) }
func (h MinHeap) Less(i, j int) bool { return h[i].Less(h[j]) }
func (h MinHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].SetIndex(i)
	h[j].SetIndex(j)
}

func (h *MinHeap) Push(x interface{}) {
	*h = append(*h, x.(HeapElement))
	n := len(*h)
	(*h)[n-1].SetIndex(n - 1)
}
func (h *MinHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	old[n-1] = nil // avoid memory leak
	*h = old[0 : n-1]
	return x
}

func NewMinHeap(size int) MinHeap {
	return MinHeap(make([]HeapElement, 0, size))
}

func (h *MinHeap) Size() int {
	return len(*h)
}

func (h *MinHeap) Insert(e HeapElement) {
	heap.Push(h, e)
}

func (h *MinHeap) Remove(e HeapElement) {
	heap.Remove(h, e.GetIndex())
}

func (h *MinHeap) Min() HeapElement {
	if len(*h) > 0 {
		return (*h)[0]
	} else {
		return nil
	}
}

//return the min element and remove it
func (h *MinHeap) PopMin() HeapElement {
	if len(*h) > 0 {
		m := (*h)[0]
		heap.Pop(h)
		return m
	} else {
		return nil
	}
}

func (h *MinHeap) Clear() {
	for i, _ := range *h {
		(*h)[i] = nil
	}
	*h = (*h)[0:0]
}
