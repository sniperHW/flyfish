package fixedarray

import (
	"sync"
)

type FixedArrayPool struct {
	pool sync.Pool
}

func NewPool(size int) *FixedArrayPool {
	return &FixedArrayPool{
		pool: sync.Pool{
			New: func() interface{} {
				return &FixedArray{
					array: make([]interface{}, size),
					len:   0,
				}
			},
		},
	}
}

func (this *FixedArrayPool) Get() *FixedArray {
	return this.pool.Get().(*FixedArray)
}

func (this *FixedArrayPool) Put(a *FixedArray) {
	a.Reset()
	this.pool.Put(a)
}

type FixedArray struct {
	len   int
	array []interface{}
}

func (this *FixedArray) Reset() {
	this.len = 0
}

func (this *FixedArray) Append(v interface{}) bool {
	if this.len < cap(this.array) {
		this.array[this.len] = v
		this.len++
		return true
	} else {
		return false
	}
}

func (this *FixedArray) Len() int {
	return this.len
}

func (this *FixedArray) Cap() int {
	return cap(this.array)
}

func (this *FixedArray) ForEach(cb func(v interface{})) {
	for i := 0; i < this.len; i++ {
		cb(this.array[i])
	}
}
