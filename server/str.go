package server

import (
	"encoding/binary"
	"github.com/sniperHW/flyfish/proto"
	"math"
	"sync"
	"unsafe"
)

const strThreshold = 1024 * 128 //128k

type str struct {
	data []byte
	len  int
	cap  int
}

func (this *str) reset() {
	this.len = 0
}

func (this *str) dataLen() int {
	return this.len
}

func (this *str) toString() string {
	tmp := this.data[:this.len]
	return *(*string)(unsafe.Pointer(&tmp))
}

func (this *str) bytes() []byte {
	return this.data[:this.len]
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

func (this *str) appendInt64(i int64) *str {
	s := 8
	newLen := this.len + s
	if newLen > this.cap {
		this.expand(s)
	}
	binary.BigEndian.PutUint64(this.data[this.len:], uint64(i))
	this.len = newLen
	return this
}

func (this *str) appendInt32(i int32) *str {
	s := 4
	newLen := this.len + s
	if newLen > this.cap {
		this.expand(s)
	}
	binary.BigEndian.PutUint32(this.data[this.len:], uint32(i))
	this.len = newLen
	return this
}

func (this *str) appendByte(i byte) *str {
	s := 1
	newLen := this.len + s
	if newLen > this.cap {
		this.expand(s)
	}
	this.data[this.len] = i
	this.len = newLen
	return this
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

func (this *str) appendField(field *proto.Field) *str {

	this.appendInt32(int32(len(field.GetName())))
	this.append(field.GetName())

	tt := field.GetType()

	switch tt {
	case proto.ValueType_string:
		this.appendByte(byte(proto.ValueType_string))
		this.appendInt32(int32(len(field.GetString())))
		this.append(field.GetString())
	case proto.ValueType_float:
		this.appendByte(byte(proto.ValueType_float))
		u64 := math.Float64bits(field.GetFloat())
		this.appendInt64(int64(u64))
	case proto.ValueType_int:
		this.appendByte(byte(proto.ValueType_int))
		this.appendInt64(field.GetInt())
	case proto.ValueType_uint:
		this.appendByte(byte(proto.ValueType_uint))
		this.appendInt64(int64(field.GetUint()))
	case proto.ValueType_blob:
		this.appendByte(byte(proto.ValueType_blob))
		this.appendInt32(int32(len(field.GetBlob())))
		this.appendBytes(field.GetBlob()...)
	default:
		panic("invaild value type")
	}

	return this

	return this
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
			data: make([]byte, strThreshold),
			cap:  strThreshold,
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
