package buffer

import (
	"encoding/binary"
	"errors"
	"unsafe"
)

var ErrBeyondSize error = errors.New("beyond size")

type Buffer struct {
	bs   []byte
	pool *Pool
}

func New(o ...[]byte) *Buffer {
	if len(o) > 0 {
		return &Buffer{
			bs: o[0],
		}
	} else {
		return &Buffer{}
	}
}

func AppendByte(bs []byte, v byte) []byte {
	return append(bs, v)
}

func AppendString(bs []byte, s string) []byte {
	return append(bs, s...)
}

func AppendBytes(bs []byte, bytes []byte) []byte {
	return append(bs, bytes...)
}

func AppendUint16(bs []byte, u16 uint16) []byte {
	bu := make([]byte, 2)
	binary.BigEndian.PutUint16(bu, u16)
	return AppendBytes(bs, bu)
}

func AppendUint32(bs []byte, u32 uint32) []byte {
	bu := make([]byte, 4)
	binary.BigEndian.PutUint32(bu, u32)
	return AppendBytes(bs, bu)
}

func AppendUint64(bs []byte, u64 uint64) []byte {
	bu := make([]byte, 8)
	binary.BigEndian.PutUint64(bu, u64)
	return AppendBytes(bs, bu)
}

func AppendInt16(bs []byte, i16 int16) []byte {
	return AppendUint16(bs, uint16(i16))
}

func AppendInt32(bs []byte, i32 int32) []byte {
	return AppendUint32(bs, uint32(i32))
}

func AppendInt64(bs []byte, i64 int64) []byte {
	return AppendUint64(bs, uint64(i64))
}

//implement io.Writer
func (b *Buffer) Write(bytes []byte) (int, error) {
	b.AppendBytes(bytes)
	return len(bytes), nil
}

func (b *Buffer) AppendByte(v byte) *Buffer {
	b.bs = append(b.bs, v)
	return b
}

func (b *Buffer) AppendString(s string) *Buffer {
	b.bs = append(b.bs, s...)
	return b
}

func (b *Buffer) AppendBytes(bytes []byte) *Buffer {
	b.bs = append(b.bs, bytes...)
	return b
}

func (b *Buffer) AppendUint16(u16 uint16) *Buffer {
	bu := make([]byte, 2)
	binary.BigEndian.PutUint16(bu, u16)
	b.AppendBytes(bu)
	return b
}

func (b *Buffer) SetUint32(pos int, u32 uint32) *Buffer {
	bu := b.bs[pos : pos+4]
	binary.BigEndian.PutUint32(bu, u32)
	return b
}

func (b *Buffer) AppendUint32(u32 uint32) *Buffer {
	bu := make([]byte, 4)
	binary.BigEndian.PutUint32(bu, u32)
	b.AppendBytes(bu)
	return b
}

func (b *Buffer) AppendUint64(u64 uint64) *Buffer {
	bu := make([]byte, 8)
	binary.BigEndian.PutUint64(bu, u64)
	b.AppendBytes(bu)
	return b
}

func (b *Buffer) AppendInt16(i16 int16) *Buffer {
	b.AppendUint16(uint16(i16))
	return b
}

func (b *Buffer) AppendInt32(i32 int32) *Buffer {
	b.AppendUint32(uint32(i32))
	return b
}

func (b *Buffer) AppendInt64(i64 int64) *Buffer {
	b.AppendUint64(uint64(i64))
	return b
}

func (b *Buffer) Bytes() []byte {
	return b.bs
}

func (b *Buffer) Len() int {
	return len(b.bs)
}

func (b *Buffer) Cap() int {
	return cap(b.bs)
}

func (b *Buffer) Reset() {
	b.bs = b.bs[:0]
}

//将len设置为l,丢弃之后的字节
func (b *Buffer) SetLen(l int) *Buffer {
	if l < len(b.bs) {
		b.bs = b.bs[:l]
	}
	return b
}

//丢弃前面n个字节
func (b *Buffer) DropFirstNBytes(n int) *Buffer {
	if n > 0 && n <= len(b.bs) {
		if n == len(b.bs) {
			b.bs = b.bs[:0]
		} else {
			copy(b.bs, b.bs[n:])
			b.bs = b.bs[:len(b.bs)-n]
		}
	}
	return b
}

func (b *Buffer) Free() {
	if nil != b.pool {
		b.pool.Put(b)
	}
}

func (b *Buffer) ToStrUnsafe() string {
	return *(*string)(unsafe.Pointer(&b.bs))
}

type BufferReader struct {
	bs     []byte
	offset int
}

func NewReader(b interface{}) BufferReader {
	switch b.(type) {
	case *Buffer:
		return BufferReader{bs: b.(*Buffer).bs}
	case []byte:
		return BufferReader{bs: b.([]byte)}
	default:
	}
	return BufferReader{}
}

func (this *BufferReader) GetAll() []byte {
	return this.bs[this.offset:]
}

func (this *BufferReader) GetOffset() int {
	return this.offset
}

func (this *BufferReader) IsOver() bool {
	return this.offset >= len(this.bs)
}

func (this *BufferReader) GetByte() byte {
	if this.offset+1 > len(this.bs) {
		return 0
	} else {
		ret := this.bs[this.offset]
		this.offset += 1
		return ret
	}
}

func (this *BufferReader) CheckGetByte() (byte, error) {
	if this.offset+1 > len(this.bs) {
		return 0, ErrBeyondSize
	} else {
		ret := this.bs[this.offset]
		this.offset += 1
		return ret, nil
	}
}

func (this *BufferReader) GetUint16() uint16 {
	if this.offset+2 > len(this.bs) {
		return 0
	} else {
		ret := binary.BigEndian.Uint16(this.bs[this.offset : this.offset+2])
		this.offset += 2
		return ret
	}
}

func (this *BufferReader) CheckGetUint16() (uint16, error) {
	if this.offset+2 > len(this.bs) {
		return 0, ErrBeyondSize
	} else {
		ret := binary.BigEndian.Uint16(this.bs[this.offset : this.offset+2])
		this.offset += 2
		return ret, nil
	}
}

func (this *BufferReader) GetInt16() int16 {
	return int16(this.GetUint16())
}

func (this *BufferReader) CheckGetInt16() (int16, error) {
	u, err := this.CheckGetUint16()
	if nil != err {
		return 0, err
	} else {
		return int16(u), nil
	}
}

func (this *BufferReader) GetUint32() uint32 {
	if this.offset+4 > len(this.bs) {
		return 0
	} else {
		ret := binary.BigEndian.Uint32(this.bs[this.offset : this.offset+4])
		this.offset += 4
		return ret
	}
}

func (this *BufferReader) CheckGetUint32() (uint32, error) {
	if this.offset+4 > len(this.bs) {
		return 0, ErrBeyondSize
	} else {
		ret := binary.BigEndian.Uint32(this.bs[this.offset : this.offset+4])
		this.offset += 4
		return ret, nil
	}
}

func (this *BufferReader) GetInt32() int32 {
	return int32(this.GetUint32())
}

func (this *BufferReader) CheckGetInt32() (int32, error) {
	u, err := this.CheckGetUint32()
	if nil != err {
		return 0, err
	} else {
		return int32(u), nil
	}
}

func (this *BufferReader) GetUint64() uint64 {
	if this.offset+8 > len(this.bs) {
		return 0
	} else {
		ret := binary.BigEndian.Uint64(this.bs[this.offset : this.offset+8])
		this.offset += 8
		return ret
	}
}

func (this *BufferReader) CheckGetUint64() (uint64, error) {
	if this.offset+8 > len(this.bs) {
		return 0, ErrBeyondSize
	} else {
		ret := binary.BigEndian.Uint64(this.bs[this.offset : this.offset+8])
		this.offset += 8
		return ret, nil
	}
}

func (this *BufferReader) GetInt64() int64 {
	return int64(this.GetUint64())
}

func (this *BufferReader) CheckGetInt64() (int64, error) {
	u, err := this.CheckGetUint64()
	if nil != err {
		return 0, err
	} else {
		return int64(u), nil
	}
}

func (this *BufferReader) GetString(size int) string {
	return string(this.GetBytes(size))
}

func (this *BufferReader) CheckGetString(size int) (string, error) {
	b, err := this.CheckGetBytes(size)
	if nil != err {
		return "", err
	} else {
		return string(b), nil
	}
}

func (this *BufferReader) GetBytes(size int) []byte {
	if len(this.bs)-this.offset < size {
		size = len(this.bs) - this.offset
	}
	ret := this.bs[this.offset : this.offset+size]
	this.offset += size
	return ret
}

func (this *BufferReader) CheckGetBytes(size int) ([]byte, error) {
	if len(this.bs)-this.offset < size {
		return nil, ErrBeyondSize
	}
	ret := this.bs[this.offset : this.offset+size]
	this.offset += size
	return ret, nil
}

func (this *BufferReader) CopyBytes(size int) ([]byte, error) {
	if b, err := this.CheckGetBytes(size); nil != err {
		out := make([]byte, len(b))
		copy(out, b)
		return out, nil
	} else {
		return nil, err
	}
}
