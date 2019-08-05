package flyfish

import (
	"sync"
	"unsafe"
)

const strInitCap = 1024 * 1024

func isPow2(size int) bool {
	return (size & (size - 1)) == 0
}

func sizeofPow2(size int) int {
	if isPow2(size) {
		return size
	}
	size = size - 1
	size = size | (size >> 1)
	size = size | (size >> 2)
	size = size | (size >> 4)
	size = size | (size >> 8)
	size = size | (size >> 16)
	return size + 1
}

type str struct {
	data []byte
	len  int
	cap  int
}

func (this *str) reset() {
	this.len = 0
}

func (this *str) toString() string {
	tmp := this.data[:this.len]
	return *(*string)(unsafe.Pointer(&tmp))
}

func (this *str) expand(need int) {
	newCap := sizeofPow2(this.len + need)
	data := make([]byte, newCap)
	if this.len > 0 {
		copy(data, this.data[:this.len])
	}
	this.data = data
	this.cap = newCap
}

func (this *str) appendBytes(bytes ...byte) *str {
	s := len(bytes)
	if 0 == s {
		return this
	} else {
		newLen := this.len + s
		if newLen > this.cap {
			this.expand(s)
		}
		copy(this.data[this.len:], bytes[:])
		this.len = newLen
		return this
	}
}

func (this *str) append(in string) *str {
	s := len(in)
	newLen := this.len + s
	if newLen > this.cap {
		this.expand(s)
	}
	copy(this.data[this.len:], in[:])
	this.len = newLen
	return this
}

func (this *str) join(other []*str, sep string) *str {
	if len(other) > 0 {
		for i, v := range other {
			if i != 0 {
				this.append(sep).append(v.toString())
			} else {
				this.append(v.toString())
			}
		}
	}
	return this
}

var strPool = sync.Pool{
	New: func() interface{} {
		return &str{
			data: make([]byte, strInitCap),
			cap:  strInitCap,
			len:  0,
		}
	},
}

func strGet() *str {
	return strPool.Get().(*str)
}

func strPut(s *str) {
	s.reset()
	strPool.Put(s)
}
