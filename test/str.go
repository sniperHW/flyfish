package main

import (
	"fmt"
	"strings"
)

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
	return string(this.data[:this.len])
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

func main() {
	s := str{
		data: make([]byte, 10),
		len:  0,
		cap:  10,
	}

	ss := []*str{}
	for i := 0; i < 10; i++ {
		s := &str{
			data: make([]byte, 10),
			len:  0,
			cap:  10,
		}
		s.append("a")
		ss = append(ss, s)
	}

	s.join(ss, ",")
	fmt.Println(s.toString())

	v := strings.Split("1213:123213:", ":")

	fmt.Println(len(v), v[0], v[1], v[2])

	/*s.append("hello")

	fmt.Println(s.toString())

	s.append("world!")

	fmt.Println(s.toString(),s.len,s.cap)*/

}
