package client

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	protocol "github.com/sniperHW/flyfish/proto"
	"reflect"
	"unsafe"
)

const CompressSize = 16 * 1024 //对超过这个大小的blob字段执行压缩

type Field protocol.Field

func (this *Field) IsNil() bool {
	return (*protocol.Field)(this).IsNil()
}

func (this *Field) GetString() string {
	return (*protocol.Field)(this).GetString()
}

func (this *Field) GetInt() int64 {
	return (*protocol.Field)(this).GetInt()
}

func (this *Field) GetFloat() float64 {
	return (*protocol.Field)(this).GetFloat()
}

func (this *Field) GetBlob() []byte {
	return (*protocol.Field)(this).GetBlob()
}

func (this *Field) GetValue() interface{} {
	return (*protocol.Field)(this).GetValue()
}

func UnmarshalJsonField(field *Field, obj interface{}) error {
	if field == nil {
		return nil
	} else {
		v := field.GetValue()
		switch v.(type) {
		case string, []byte:
			var b []byte
			switch v.(type) {
			case []byte:
				b = v.([]byte)
			case string:
				s := v.(string)
				b = *(*[]byte)(unsafe.Pointer(&reflect.SliceHeader{
					Len:  int(len(s)),
					Cap:  int(len(s)),
					Data: (*reflect.StringHeader)(unsafe.Pointer(&s)).Data,
				}))
			}

			if len(b) == 0 {
				return nil
			} else {
				return json.Unmarshal(b, obj)
			}
		default:
			return nil
		}
	}
}

//对大小>=1k的[]byte字段，执行压缩
func PackField(key string, v interface{}) *protocol.Field {
	switch v.(type) {
	case []byte:
		b := v.([]byte)
		var bb []byte
		if len(b) >= CompressSize {
			bb, _ = getCompressor().Compress(b)
			size := make([]byte, 4)
			binary.BigEndian.PutUint32(size, uint32(len(bb)+4))
			bb = append(bb, size...)
		} else {
			bb = b
		}
		return protocol.PackField(key, bb)
	default:
		return protocol.PackField(key, v)
	}
}

func UnpackField(f *protocol.Field) (*Field, error) {
	var err error
	if nil != f {
		switch f.GetValue().(type) {
		case []byte:
			b := f.GetBlob()
			if ok, size := checkHeader(b); ok {
				if len(b) >= size+4 {
					if size = int(binary.BigEndian.Uint32(b[len(b)-4:])); size == len(b) {
						if b, err = getDecompressor().Decompress(b[:len(b)-4]); nil == err {
							return (*Field)(protocol.PackField(f.Name, b)), err
						}
					} else {
						err = errors.New("flyfish client unpackField:invaild filed1")
					}
				} else {
					err = errors.New("flyfish client unpackField:invaild filed2")
				}

				if nil != err {
					return (*Field)(protocol.PackField(f.Name, []byte{})), err
				}
			}
		}
	}
	return (*Field)(f), err
}
