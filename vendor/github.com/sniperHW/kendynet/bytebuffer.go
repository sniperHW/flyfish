package kendynet

import (
	"encoding/binary"
	"fmt"
	"reflect"
)

func IsPow2(size uint64) bool {
	return (size & (size - 1)) == 0
}

func SizeofPow2(size uint64) uint64 {
	if IsPow2(size) {
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

func GetPow2(size uint64) uint64 {
	var pow2 uint64 = 0
	if !IsPow2(size) {
		size = (size << 1)
	}
	for size > 1 {
		pow2++
	}
	return pow2
}

const (
	MaxBuffSize uint64 = 0xFFFFFFFF
)

type ByteBuffer struct {
	buffer   []byte
	datasize uint64
	capacity uint64
	needcopy bool //标记是否执行写时拷贝
}

func NewByteBuffer(arg ...interface{}) *ByteBuffer {
	if len(arg) < 2 {
		var size uint64
		if len(arg) == 0 {
			size = 128
		} else {
			switch arg[0].(type) {
			case int8:
				size = (uint64)(arg[0].(int8))
				break
			case uint8:
				size = (uint64)(arg[0].(uint8))
				break
			case int16:
				size = (uint64)(arg[0].(int16))
				break
			case uint16:
				size = (uint64)(arg[0].(uint16))
				break
			case int:
				size = (uint64)(arg[0].(int))
				break
			case int32:
				size = (uint64)(arg[0].(int32))
				break
			case uint32:
				size = (uint64)(arg[0].(uint32))
				break
			case int64:
				size = (uint64)(arg[0].(int64))
				break
			case uint64:
				size = (uint64)(arg[0].(uint64))
				break
			case string:
				data := arg[0].(string)
				datasize := (uint64)(len(data))
				size = SizeofPow2(datasize)
				if size < 128 {
					size = 128
				}
				buff := &ByteBuffer{buffer: make([]byte, size), datasize: datasize, capacity: size, needcopy: false}
				buff.PutString(0, data)
				return buff
			case []byte:
				data := arg[0].([]byte)
				datasize := (uint64)(len(data))
				size = SizeofPow2(datasize)
				if size < 128 {
					size = 128
				}
				buff := &ByteBuffer{buffer: make([]byte, size), datasize: datasize, capacity: size, needcopy: false}
				buff.PutBytes(0, data)
				return buff
			default:
				fmt.Printf("invaild %s\n", reflect.TypeOf(arg[0]).String())
				return nil
			}
		}
		size = SizeofPow2(size)
		if size < 128 {
			size = 128
		}
		return &ByteBuffer{buffer: make([]byte, size), datasize: 0, capacity: size, needcopy: false}
	} else if len(arg) == 2 {
		var bytes []byte
		var size uint64
		switch arg[0].(type) {
		case []byte:
			bytes = arg[0].([]byte)
			break
		default:
			fmt.Printf("invaild %s\n", reflect.TypeOf(arg[0]).String())
			return nil
		}
		switch arg[1].(type) {
		case int8:
			size = (uint64)(arg[1].(int8))
			break
		case uint8:
			size = (uint64)(arg[1].(uint8))
			break
		case int16:
			size = (uint64)(arg[1].(int16))
			break
		case uint16:
			size = (uint64)(arg[1].(uint16))
			break
		case int:
			size = (uint64)(arg[1].(int))
			break
		case int32:
			size = (uint64)(arg[1].(int32))
			break
		case uint32:
			size = (uint64)(arg[1].(uint32))
			break
		case int64:
			size = (uint64)(arg[1].(int64))
			break
		case uint64:
			size = (uint64)(arg[1].(uint64))
			break
		default:
			fmt.Printf("invaild %s\n", reflect.TypeOf(arg[0]).String())
			return nil
		}
		/*
		 * 直接引用bytes,并设置needcopy标记
		 * 如果ByteBuffer要修改bytes中的内容，首先要先执行拷贝，之后才能修改
		 */
		return &ByteBuffer{buffer: bytes, datasize: size, capacity: size, needcopy: true}
	} else {
		return nil
	}
}

func (this *ByteBuffer) Reset() {
	this.datasize = 0
}

func (this *ByteBuffer) Clone() *ByteBuffer {
	b := make([]byte, this.capacity)
	copy(b[0:], this.buffer[:this.capacity])
	return &ByteBuffer{buffer: b, datasize: this.datasize, capacity: this.capacity, needcopy: false}
}

/*
func (this *ByteBuffer) Buffer()([]byte){
	return this.buffer
}
*/

func (this *ByteBuffer) Len() uint64 {
	return this.datasize
}

func (this *ByteBuffer) Cap() uint64 {
	return this.capacity
}

func (this *ByteBuffer) expand(newsize uint64) error {
	newsize = SizeofPow2(newsize)
	if newsize > MaxBuffSize {
		return ErrBuffMaxSizeExceeded
	}
	//allocate new buffer
	tmpbuf := make([]byte, newsize)
	//copy data
	copy(tmpbuf[0:], this.buffer[:this.datasize])
	//replace buffer
	this.buffer = tmpbuf
	this.capacity = newsize
	return nil
}

func (this *ByteBuffer) checkCapacity(idx, size uint64) error {
	if size >= this.capacity && idx+size < this.capacity {
		//溢出
		return ErrBuffMaxSizeExceeded
	}

	if this.needcopy {
		//需要执行写时拷贝
		sizeneed := idx + size
		if sizeneed > MaxBuffSize {
			return ErrBuffMaxSizeExceeded
		}
		//allocate new buffer
		tmpbuf := make([]byte, sizeneed)
		//copy data
		copy(tmpbuf[0:], this.buffer[:this.datasize])
		//replace buffer
		this.buffer = tmpbuf
		this.capacity = sizeneed
		this.needcopy = false
		return nil
	}
	if idx+size > this.capacity {
		err := this.expand(idx + size)
		if err != nil {
			return err
		}
	}
	return nil
}

func (this *ByteBuffer) PutBytes(idx uint64, value []byte) error {
	sizeneed := (uint64)(len(value))
	err := this.checkCapacity(idx, sizeneed)
	if err != nil {
		return err
	}
	copy(this.buffer[idx:], value[:sizeneed])
	if idx+sizeneed > this.datasize {
		this.datasize = idx + sizeneed
	}
	return nil
}

func (this *ByteBuffer) PutString(idx uint64, value string) error {
	sizeneed := (uint64)(len(value))
	err := this.checkCapacity(idx, sizeneed)
	if err != nil {
		return err
	}
	copy(this.buffer[idx:], value[:sizeneed])
	if idx+sizeneed > this.datasize {
		this.datasize = idx + sizeneed
	}
	return nil
}

func (this *ByteBuffer) PutByte(idx uint64, value byte) error {
	sizeneed := (uint64)(1)
	err := this.checkCapacity(idx, sizeneed)
	if err != nil {
		return err
	}
	this.buffer[idx] = value
	if idx+sizeneed > this.datasize {
		this.datasize = idx + sizeneed
	}
	return nil
}

func (this *ByteBuffer) PutUint16(idx uint64, value uint16) error {
	sizeneed := (uint64)(2)
	err := this.checkCapacity(idx, sizeneed)
	if err != nil {
		return err
	}
	binary.BigEndian.PutUint16(this.buffer[idx:idx+sizeneed], value)
	if idx+sizeneed > this.datasize {
		this.datasize = idx + sizeneed
	}
	return nil
}

func (this *ByteBuffer) PutUint32(idx uint64, value uint32) error {
	sizeneed := (uint64)(4)
	err := this.checkCapacity(idx, sizeneed)
	if err != nil {
		return err
	}
	binary.BigEndian.PutUint32(this.buffer[idx:idx+sizeneed], value)
	if idx+sizeneed > this.datasize {
		this.datasize = idx + sizeneed
	}
	return nil
}

func (this *ByteBuffer) PutUint64(idx uint64, value uint64) error {
	sizeneed := (uint64)(8)
	err := this.checkCapacity(idx, sizeneed)
	if err != nil {
		return err
	}
	binary.BigEndian.PutUint64(this.buffer[idx:idx+sizeneed], value)
	if idx+sizeneed > this.datasize {
		this.datasize = idx + sizeneed
	}
	return nil
}

func (this *ByteBuffer) GetString(idx uint64, size uint64) (ret string, err error) {
	var bytes []byte
	bytes, err = this.GetBytes(idx, size)
	if bytes != nil {
		ret = string(bytes)
	}
	return
}

func (this *ByteBuffer) GetBytes(idx uint64, size uint64) (ret []byte, err error) {
	ret = nil
	err = nil
	if size >= this.datasize && idx+size < this.datasize {
		err = ErrBuffInvaildAgr
		return
	}
	if idx+size > this.datasize {
		err = ErrBuffInvaildAgr
		return
	}
	ret = this.buffer[idx : idx+size]
	return
}

func (this *ByteBuffer) GetByte(idx uint64) (ret byte, err error) {
	err = nil
	size := (uint64)(1)
	if size >= this.datasize && idx+size < this.datasize {
		err = ErrBuffInvaildAgr
		return
	}
	if idx+size > this.datasize {
		err = ErrBuffInvaildAgr
		return
	}
	ret = this.buffer[idx]
	return
}

func (this *ByteBuffer) GetUint16(idx uint64) (ret uint16, err error) {
	err = nil
	size := (uint64)(2)
	if size >= this.datasize && idx+size < this.datasize {
		err = ErrBuffInvaildAgr
		return
	}
	if idx+size > this.datasize {
		err = ErrBuffInvaildAgr
		return
	}
	ret = binary.BigEndian.Uint16(this.buffer[idx : idx+size])
	return
}

func (this *ByteBuffer) GetUint32(idx uint64) (ret uint32, err error) {
	err = nil
	size := (uint64)(4)
	if size >= this.datasize && idx+size < this.datasize {
		err = ErrBuffInvaildAgr
		return
	}
	if idx+size > this.datasize {
		err = ErrBuffInvaildAgr
		return
	}
	ret = binary.BigEndian.Uint32(this.buffer[idx : idx+size])
	return
}

func (this *ByteBuffer) GetUint64(idx uint64) (ret uint64, err error) {
	err = nil
	size := (uint64)(8)
	if size >= this.datasize && idx+size < this.datasize {
		err = ErrBuffInvaildAgr
		return
	}
	if idx+size > this.datasize {
		err = ErrBuffInvaildAgr
		return
	}
	ret = binary.BigEndian.Uint64(this.buffer[idx : idx+size])
	return
}

func (this *ByteBuffer) AppendBytes(value []byte) error {
	return this.PutBytes(this.datasize, value)
}

func (this *ByteBuffer) AppendString(value string) error {
	return this.PutString(this.datasize, value)
}

func (this *ByteBuffer) AppendByte(value byte) error {
	return this.PutByte(this.datasize, value)
}

func (this *ByteBuffer) AppendUint16(value uint16) error {
	return this.PutUint16(this.datasize, value)
}

func (this *ByteBuffer) AppendUint32(value uint32) error {
	return this.PutUint32(this.datasize, value)
}

func (this *ByteBuffer) AppendUint64(value uint64) error {
	return this.PutUint64(this.datasize, value)
}

func (this *ByteBuffer) Bytes() []byte {
	return this.buffer[:this.datasize]
}


type BufferReader struct {
	buffer *ByteBuffer
	offset  uint64
}

func NewReader(buffer *ByteBuffer) *BufferReader {
	return &BufferReader{buffer:buffer}
}

func (this *BufferReader) GetByte() (ret byte, err error) {
	ret,err = this.buffer.GetByte(this.offset)
	if nil == err {
		this.offset += 1
	}
	return
}

func (this *BufferReader) GetUint16() (ret uint16, err error) {
	ret,err = this.buffer.GetUint16(this.offset)
	if nil == err {
		this.offset += 2
	}
	return
}

func (this *BufferReader) GetUint32() (ret uint32, err error) {
	ret,err = this.buffer.GetUint32(this.offset)
	if nil == err {
		this.offset += 4
	}
	return
}

func (this *BufferReader) GetUint64() (ret uint64, err error) {
	ret,err = this.buffer.GetUint64(this.offset)
	if nil == err {
		this.offset += 8
	}
	return
}

func (this *BufferReader) GetString(size uint64) (ret string, err error) {
	ret,err = this.buffer.GetString(this.offset,size)
	if nil == err {
		this.offset += size
	}
	return
}

func (this *BufferReader) GetBytes(size uint64) (ret []byte, err error) {
	ret,err = this.buffer.GetBytes(this.offset,size)
	if nil == err {
		this.offset += size
	}
	return
}

