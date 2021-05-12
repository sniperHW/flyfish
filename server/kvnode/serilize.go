package kvnode

import (
	"errors"
	"github.com/sniperHW/flyfish/pkg/buffer"
	flyproto "github.com/sniperHW/flyfish/proto"
	"math"
	"time"
)

type proposalReader struct {
	reader buffer.BufferReader
}

type ppkv struct {
	unikey  string
	version int64
	fields  map[string]*flyproto.Field
}

type pplease struct {
	nodeid  int
	begtime time.Time
}

func appendField(b []byte, field *flyproto.Field) []byte {
	//name len
	b = buffer.AppendUint16(b, uint16(len(field.GetName())))
	//name
	b = buffer.AppendString(b, field.GetName())

	tt := field.GetType()

	b = buffer.AppendByte(b, byte(tt))

	switch tt {
	case flyproto.ValueType_string:
		b = buffer.AppendInt32(b, int32(len(field.GetString())))
		b = buffer.AppendString(b, field.GetString())
	case flyproto.ValueType_float:
		u64 := math.Float64bits(field.GetFloat())
		b = buffer.AppendInt64(b, int64(u64))
	case flyproto.ValueType_int:
		b = buffer.AppendInt64(b, field.GetInt())
	case flyproto.ValueType_blob:
		b = buffer.AppendInt32(b, int32(len(field.GetBlob())))
		b = buffer.AppendBytes(b, field.GetBlob())
	default:
		panic("invaild field value type")
	}
	return b
}

func serilizeKv(b []byte, ptype proposalType, unikey string, version int64, fields map[string]*flyproto.Field) []byte {
	//先写入类型
	b = buffer.AppendByte(b, byte(ptype))
	//len unikey
	b = buffer.AppendUint16(b, uint16(len(unikey)))
	//unikey
	b = buffer.AppendString(b, unikey)

	if ptype != proposal_kick {
		//version
		b = buffer.AppendInt64(b, version)
		//fields
		if nil != fields {
			b = buffer.AppendInt32(b, int32(len(fields)))
			for _, v := range fields {
				b = appendField(b, v)
			}
		} else {
			b = buffer.AppendInt32(b, 0)
		}
	}
	return b
}

func newProposalReader(b []byte) proposalReader {
	return proposalReader{
		reader: buffer.NewReader(b),
	}
}

func (this *proposalReader) readField() (*flyproto.Field, error) {
	var err error
	var lname uint16
	var name string
	var tt byte
	lname, err = this.reader.CheckGetUint16()
	if nil != err {
		return nil, err
	}
	name, err = this.reader.CheckGetString(int(lname))
	if nil != err {
		return nil, err
	}

	tt, err = this.reader.CheckGetByte()
	if nil != err {
		return nil, err
	}
	switch flyproto.ValueType(tt) {
	case flyproto.ValueType_int, flyproto.ValueType_float:
		//GetSugar().Infof("readField %s number", name)
		var i int64
		i, err = this.reader.CheckGetInt64()
		if nil != err {
			return nil, err
		} else if flyproto.ValueType(tt) == flyproto.ValueType_int {
			return flyproto.PackField(name, i), nil
		} else {
			return flyproto.PackField(name, math.Float64frombits(uint64(i))), nil
		}
	case flyproto.ValueType_string, flyproto.ValueType_blob:
		var l int32
		var b []byte
		l, err = this.reader.CheckGetInt32()
		if nil != err {
			return nil, err
		}
		b, err = this.reader.CheckGetBytes(int(l))
		if nil != err {
			return nil, err
		}
		if flyproto.ValueType(tt) == flyproto.ValueType_blob {
			return flyproto.PackField(name, b), nil
		} else {
			//GetSugar().Infof("readField %s string %s", name, string(b))
			return flyproto.PackField(name, string(b)), nil
		}
	default:
	}
	return nil, errors.New("bad data 1")
}

func (this *proposalReader) read() (isOver bool, ptype proposalType, data interface{}, err error) {
	if this.reader.IsOver() {
		isOver = true
		return
	} else {
		var b byte
		b, err = this.reader.CheckGetByte()
		if nil == err {
			ptype = proposalType(b)
			switch ptype {
			case proposal_none:
				err = errors.New("bad data 2")
			case proposal_lease:
				var id int32
				id, err = this.reader.CheckGetInt32()
				if nil != err {
					return
				}
				var l int32
				l, err = this.reader.CheckGetInt32()
				if nil != err {
					return
				}
				var bb []byte
				bb, err = this.reader.CheckGetBytes(int(l))
				if nil != err {
					return
				}

				var t time.Time
				err = t.UnmarshalBinary(bb)
				if nil != err {
					return
				}

				data = pplease{
					nodeid:  int(id),
					begtime: t,
				}
				return
			case proposal_snapshot, proposal_update, proposal_kick:
				var l uint16
				l, err = this.reader.CheckGetUint16()
				if nil != err {
					return
				}
				p := ppkv{}
				p.unikey, err = this.reader.CheckGetString(int(l))
				if nil != err {
					return
				}

				if ptype != proposal_kick {

					p.version, err = this.reader.CheckGetInt64()
					if nil != err {
						return
					}
					var fieldSize int32
					fieldSize, err = this.reader.CheckGetInt32()
					if nil != err {
						return
					}

					//GetSugar().Infof("fieldSize:%d", fieldSize)

					var fields map[string]*flyproto.Field

					if fieldSize > 0 {
						fields = map[string]*flyproto.Field{}
						var field *flyproto.Field
						for i := int32(0); i < fieldSize; i++ {
							field, err = this.readField()
							if nil != err {
								return
							} else {
								fields[field.GetName()] = field
							}
						}
					}

					p.fields = fields
				}

				data = p
			default:
				err = errors.New("bad data 3")
			}
		}
		return
	}
}
