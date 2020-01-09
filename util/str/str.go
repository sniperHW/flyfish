package str

import (
	"encoding/binary"
	"fmt"
	"github.com/sniperHW/flyfish/proto"
	"github.com/sniperHW/flyfish/util"
	"math"
	"strconv"
	"strings"
	"sync"
	"unsafe"
)

const strThreshold = 1024 * 128 //128k

type Str struct {
	data []byte
	len  int
	cap  int
}

func (this *Str) Reset() {
	this.len = 0
}

func (this *Str) Len() int {
	return this.len
}

func (this *Str) ToString() string {
	tmp := this.data[:this.len]
	return *(*string)(unsafe.Pointer(&tmp))
}

func (this *Str) Bytes() []byte {
	return this.data[:this.len]
}

func (this *Str) expand(need int) {
	newCap := util.SizeOfPow2(this.len + need)
	data := make([]byte, newCap)
	if this.len > 0 {
		copy(data, this.data[:this.len])
	}
	this.data = data
	this.cap = newCap
}

func (this *Str) AppendInt64(i int64) *Str {
	s := 8
	newLen := this.len + s
	if newLen > this.cap {
		this.expand(s)
	}
	binary.BigEndian.PutUint64(this.data[this.len:], uint64(i))
	this.len = newLen
	return this
}

func (this *Str) ReadInt64(offset int) (int64, int, error) {
	if offset+8 > this.len {
		return 0, 0, fmt.Errorf("beyond size")
	}
	return int64(binary.BigEndian.Uint64(this.data[offset : offset+8])), offset + 8, nil
}

func (this *Str) AppendInt32(i int32) *Str {
	s := 4
	newLen := this.len + s
	if newLen > this.cap {
		this.expand(s)
	}
	binary.BigEndian.PutUint32(this.data[this.len:], uint32(i))
	this.len = newLen
	return this
}

func (this *Str) SetInt32(offset int, i int32) error {
	if offset+4 > this.len {
		return fmt.Errorf("beyond size")
	}
	binary.BigEndian.PutUint32(this.data[offset:offset+4], uint32(i))
	return nil
}

func (this *Str) ReadInt32(offset int) (int32, int, error) {
	if offset+4 > this.len {
		return 0, 0, fmt.Errorf("beyond size")
	}
	return int32(binary.BigEndian.Uint32(this.data[offset : offset+4])), offset + 4, nil
}

func (this *Str) AppendByte(i byte) *Str {
	s := 1
	newLen := this.len + s
	if newLen > this.cap {
		this.expand(s)
	}
	this.data[this.len] = i
	this.len = newLen
	return this
}

func (this *Str) ReadByte(offset int) (byte, int, error) {
	if offset+1 > this.len {
		return 0, 0, fmt.Errorf("beyond size")
	}
	return this.data[offset], offset + 1, nil
}

func (this *Str) AppendBytes(bytes ...byte) *Str {
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

func (this *Str) ReadBytes(offset int, size int) ([]byte, int, error) {
	if offset+size > this.len {
		return nil, 0, fmt.Errorf("beyond size")
	}
	return this.data[offset : offset+size], offset + size, nil
}

func (this *Str) AppendFieldStr(field *proto.Field, binaryToSqlStr func(*Str, []byte)) *Str {
	tt := field.GetType()

	switch tt {
	case proto.ValueType_string:
		this.AppendString(fmt.Sprintf("'%s'", field.GetString()))
	case proto.ValueType_float:
		this.AppendString(fmt.Sprintf("%f", field.GetFloat()))
	case proto.ValueType_int:
		this.AppendString(strconv.FormatInt(field.GetInt(), 10))
	case proto.ValueType_uint:
		this.AppendString(strconv.FormatUint(field.GetUint(), 10))
	case proto.ValueType_blob:
		binaryToSqlStr(this, field.GetBlob())
	}

	return this
}

func (this *Str) AppendField(field *proto.Field) *Str {

	this.AppendInt32(int32(len(field.GetName())))
	this.AppendString(field.GetName())

	tt := field.GetType()

	switch tt {
	case proto.ValueType_string:
		this.AppendByte(byte(proto.ValueType_string))
		this.AppendInt32(int32(len(field.GetString())))
		this.AppendString(field.GetString())
	case proto.ValueType_float:
		this.AppendByte(byte(proto.ValueType_float))
		u64 := math.Float64bits(field.GetFloat())
		this.AppendInt64(int64(u64))
	case proto.ValueType_int:
		this.AppendByte(byte(proto.ValueType_int))
		this.AppendInt64(field.GetInt())
	case proto.ValueType_uint:
		this.AppendByte(byte(proto.ValueType_uint))
		this.AppendInt64(int64(field.GetUint()))
	case proto.ValueType_blob:
		this.AppendByte(byte(proto.ValueType_blob))
		this.AppendInt32(int32(len(field.GetBlob())))
		this.AppendBytes(field.GetBlob()...)
	default:
		panic("invaild value type")
	}

	return this
}

func (this *Str) ReadField(offset int) (*proto.Field, int, error) {
	var err error
	var nameLen int32

	nameLen, offset, err = this.ReadInt32(offset)
	if nil != err {
		return nil, 0, err
	}

	var name string
	name, offset, err = this.ReadString(offset, int(nameLen))
	if nil != err {
		return nil, 0, err
	}

	var tt byte

	tt, offset, err = this.ReadByte(offset)
	if nil != err {
		return nil, 0, err
	}

	switch proto.ValueType(tt) {
	case proto.ValueType_string:
		var strLen int32
		strLen, offset, err = this.ReadInt32(offset)
		if nil != err {
			return nil, 0, err
		}
		var str string
		str, offset, err = this.ReadString(offset, int(strLen))
		if nil != err {
			return nil, 0, err
		}

		return proto.PackField(name, str), offset, nil
	case proto.ValueType_float:
		var i64 int64
		i64, offset, err = this.ReadInt64(offset)
		if nil != err {
			return nil, 0, err
		}
		return proto.PackField(name, math.Float64frombits(uint64(i64))), offset, nil
	case proto.ValueType_int:
		var i64 int64
		i64, offset, err = this.ReadInt64(offset)
		if nil != err {
			return nil, 0, err
		}
		return proto.PackField(name, i64), offset, nil
	case proto.ValueType_uint:
		var i64 int64
		i64, offset, err = this.ReadInt64(offset)
		if nil != err {
			return nil, 0, err
		}
		return proto.PackField(name, uint64(i64)), offset, nil
	case proto.ValueType_blob:
		var bytesLen int32
		bytesLen, offset, err = this.ReadInt32(offset)
		if nil != err {
			return nil, 0, err
		}

		var bytes []byte
		bytes, offset, err = this.ReadBytes(offset, int(bytesLen))
		if nil != err {
			return nil, 0, err
		}

		return proto.PackField(name, bytes), offset, nil
	default:
		return nil, 0, fmt.Errorf("invaild tt")
	}
}

func (this *Str) AppendString(in string) *Str {
	s := len(in)
	newLen := this.len + s
	if newLen > this.cap {
		this.expand(s)
	}
	copy(this.data[this.len:], in[:])
	this.len = newLen
	return this
}

func (this *Str) ReadString(offset int, size int) (string, int, error) {
	if offset+size > this.len {
		return "", 0, fmt.Errorf("beyond size")
	}
	return string(this.data[offset : offset+size]), offset + size, nil
}

func (this *Str) Join(other []*Str, sep string) *Str {
	if len(other) > 0 {
		for i, v := range other {
			if i != 0 {
				this.AppendString(sep).AppendString(v.ToString())
			} else {
				this.AppendString(v.ToString())
			}
		}
	}
	return this
}

func NewStr(buff []byte, len int) *Str {
	return &Str{
		data: buff,
		cap:  cap(buff),
		len:  len,
	}
}

var strPool = sync.Pool{
	New: func() interface{} {
		return &Str{
			data: make([]byte, strThreshold),
			cap:  strThreshold,
			len:  0,
		}
	},
}

func Get() *Str {
	return strPool.Get().(*Str)
}

func Put(s *Str) {
	s.Reset()
	strPool.Put(s)
}
