package ringqueue

/*
 *  非多线程安全循环队列
 */

import (
//"fmt"
)

type Queue struct {
	r    int
	w    int
	data []interface{}
}

func (this *Queue) Append(v interface{}) bool {
	w := (this.w + 1) % cap(this.data)

	//fmt.Println(w, this.w, this.r, cap(this.data))

	if w == this.r {
		return false
	} else {
		this.data[this.w] = v
		this.w = w
		return true
	}
}

func (this *Queue) Front() interface{} {
	if this.r == this.w {
		return nil
	} else {
		return this.data[this.r]
	}
}

func (this *Queue) PopFront() interface{} {
	if this.r == this.w {
		return nil
	} else {
		v := this.data[this.r]
		this.data[this.r] = nil
		this.r = (this.r + 1) % cap(this.data)
		return v
	}
}

func New(capacity int) *Queue {
	return &Queue{
		r:    0,
		w:    0,
		data: make([]interface{}, capacity+1),
	}
}
