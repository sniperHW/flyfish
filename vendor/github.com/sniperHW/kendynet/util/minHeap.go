package util

import(
	"fmt"
)

type HeapElement interface {
	//if self less than other return true
	Less(HeapElement)bool
	GetIndex()uint32
	SetIndex(uint32)
}


func parent(idx uint32) uint32 {
	return idx/2
}

func left(idx uint32) uint32 {
	return idx*2
}

func right(idx uint32) uint32 {
	return idx*2+1
}

type MinHeap struct {
	size uint32
	data []HeapElement
}

func NewMinHeap(size uint32) *MinHeap {
	m := &MinHeap{}
	m.data = make([]HeapElement,size + 1)
	return m
}

func (this *MinHeap) Size() uint32 {
	return this.size
}

func (this *MinHeap) swap(idx1,idx2 uint32) {
	if idx1 != idx2 {
		ele := this.data[idx1]
		this.data[idx1] = this.data[idx2]
		this.data[idx2] = ele
		this.data[idx1].SetIndex(idx1)
		this.data[idx2].SetIndex(idx2)
	}
}

func (this *MinHeap) up(idx uint32) {
	p := parent(idx)
	for p > 0 {
		if this.data[idx].Less(this.data[p]) {
			this.swap(idx,p)
			idx = p
			p = parent(idx)
		} else {
			break
		}
	}
}


func (this *MinHeap) down(idx uint32) {
	l   := left(idx)
	r   := right(idx)
	min := idx

	if l <= this.size && this.data[l].Less(this.data[idx]) {
		min = l
	}

	if r <= this.size && this.data[r].Less(this.data[min]) {
		min = r
	}

	if min != idx {
		this.swap(idx,min)
		this.down(min)
	}	
}

func (this *MinHeap) change(e HeapElement) {
	idx := e.GetIndex()
	if idx <= this.size && this.data[idx] == e {
		this.down(idx)
		if idx == e.GetIndex() {
			this.up(idx)
		}
	}
}


func (this *MinHeap) Insert(e HeapElement) {
	idx := e.GetIndex()
	if idx > 0 {
		this.change(e)
		return
	} else {

		if this.size >= uint32(cap(this.data) - 1) {
			//expand
			newSize := uint32((cap(this.data) - 1) * 2)
			data := make([]HeapElement,newSize)
			copy(data,this.data[:])
			this.data = data
		}
		this.size += 1
		this.data[this.size] = e
		e.SetIndex(this.size)
		this.up(e.GetIndex())
	}
}

func (this *MinHeap) Remove(e HeapElement) error {
	idx := e.GetIndex()
	if idx <= this.size && this.data[idx] == e {
		oldSize := this.size
		tail := this.data[this.size]
		if tail == e {
			this.size -= 1			
		} else {
			this.swap(idx,tail.GetIndex())
			this.size -= 1
			this.down(tail.GetIndex())
		}
		e.SetIndex(0)
		this.data[oldSize] = nil
		return nil
	} else {
		return fmt.Errorf("invaild element")
	}
}




func (this *MinHeap) Min() HeapElement {
	if this.size > 0 {
		return this.data[1]
	} else {
		return nil
	}
}

//return the min element and remove it
func (this *MinHeap) PopMin() HeapElement {
	if this.size > 0 {
		min := this.data[1]
		this.swap(1,this.size)
		this.data[this.size] = nil
		this.size -= 1
		this.down(1)
		min.SetIndex(0)
		return min
	} else {
		return nil
	}
}

func (this *MinHeap) Clear() {

	for i := uint32(1); i <= this.size; i++ {
		this.data[i].SetIndex(0)
		this.data[i] = nil
	}

	this.size = 0
}