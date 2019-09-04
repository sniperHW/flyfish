package proto

import (
	"fmt"
	"reflect"

	"github.com/golang/protobuf/proto"
)

func (m *Field) GetType() ValueType {
	if m.V == nil {
		return ValueType_nil
	}
	return m.V.GetType()
}

func (m *Field) IsNil() bool {
	return m.GetType() == ValueType_nil
}

func (m *Field) IsString() bool {
	return m.GetType() == ValueType_string
}

func (m *Field) IsInt() bool {
	return m.GetType() == ValueType_int
}

func (m *Field) IsUint() bool {
	return m.GetType() == ValueType_uint
}

func (m *Field) IsFloat() bool {
	return m.GetType() == ValueType_float
}

func (m *Field) IsBlob() bool {
	return m.GetType() == ValueType_blob
}

func (m *Field) GetValue() interface{} {
	return UnpackField(m)
}

func (m *Field) GetString() string {

	if !m.IsString() {
		panic("v is not string")
	}

	return m.V.GetS()
}

func (m *Field) GetBlob() []byte {

	if !m.IsBlob() {
		panic("v is not blob")
	}

	return m.V.GetB()
}

func (m *Field) GetUint() uint64 {

	if !m.IsUint() {
		panic("v is not uint")
	}

	return m.V.GetU()
}

func (m *Field) GetInt() int64 {
	if !m.IsInt() {
		panic("v is not int")
	}

	return m.V.GetI()
}

func (m *Field) GetFloat() float64 {
	if !m.IsFloat() {
		panic("v is not float")
	}

	return m.V.GetF()
}

func (m *Field) SetInt(v int64) {
	if !m.IsInt() {
		panic("v is not int")
	}
	m.V.I = proto.Int64(v)
}

func (m *Field) SetUint(v uint64) {
	if !m.IsUint() {
		panic("v is not uint")
	}
	m.V.U = proto.Uint64(v)
}

func (m *Field) SetString(v string) {
	if !m.IsString() {
		panic("v is not string")
	}
	m.V.S = proto.String(v)
}

func (m *Field) SetFloat(v float64) {
	if !m.IsFloat() {
		panic("v is not float")
	}
	m.V.F = proto.Float64(v)
}

func (m *Field) SetBlob(v []byte) {
	if !m.IsBlob() {
		panic("v is not Blob")
	}
	m.V.B = v
}

func (m *Field) Equal(o *Field) bool {
	if nil == o {
		return false
	} else if m.GetType() != o.GetType() {
		return false
	}

	switch m.GetType() {
	case ValueType_string:
		return m.GetString() == o.GetString()
	case ValueType_float:
		return m.GetFloat() == o.GetFloat()
	case ValueType_int:
		return m.GetInt() == o.GetInt()
	case ValueType_uint:
		return m.GetUint() == o.GetUint()
	case ValueType_blob:
		mm := m.GetBlob()
		oo := o.GetBlob()
		if len(mm) != len(oo) {
			return false
		}
		for i := 0; i < len(mm); i++ {
			if mm[i] != oo[i] {
				return false
			}
		}
		return true
	case ValueType_nil:
		return true
	default:
		return false
	}
}

func UnpackField(filed *Field) interface{} {
	if nil == filed {
		return nil
	}

	switch filed.GetType() {
	case ValueType_string:
		return filed.GetString()
	case ValueType_int:
		return filed.GetInt()
	case ValueType_uint:
		return filed.GetUint()
	case ValueType_float:
		return filed.GetFloat()
	case ValueType_blob:
		return filed.GetBlob()
	default:
		return nil
	}
}

func GetValueType(v interface{}) ValueType {
	if nil == v {
		return ValueType_nil
	}

	switch v.(type) {
	case string:
		return ValueType_string
	case int, int8, int16, int32, int64:
		return ValueType_int
	case uint, uint8, uint16, uint32, uint64:
		return ValueType_uint
	case float32, float64:
		return ValueType_float
	case []byte:
		return ValueType_blob
	default:
		return ValueType_invaild
	}
	return ValueType_invaild
}

func PackField(name string, v interface{}) *Field {

	field := &Field{
		V:    &Value{},
		Name: proto.String(name),
	}

	if nil == v {
		field.V.Type = ValueType(ValueType_nil).Enum()
		return field
	}

	var vvI int64
	var vvF float64
	var vvU uint64

	switch v.(type) {
	case []byte:
		field.V.B = v.([]byte)
		field.V.Type = ValueType(ValueType_blob).Enum()
		return field
	case string:
		field.V.S = proto.String(v.(string))
		field.V.Type = ValueType(ValueType_string).Enum()
		return field
	case int:
		field.V.Type = ValueType(ValueType_int).Enum()
		vvI = int64(v.(int))
		break
	case int8:
		field.V.Type = ValueType(ValueType_int).Enum()
		vvI = int64(v.(int8))
		break
	case int16:
		field.V.Type = ValueType(ValueType_int).Enum()
		vvI = int64(v.(int16))
		break
	case int32:
		field.V.Type = ValueType(ValueType_int).Enum()
		vvI = int64(v.(int32))
		break
	case int64:
		field.V.Type = ValueType(ValueType_int).Enum()
		vvI = int64(v.(int64))
		break
	case uint:
		field.V.Type = ValueType(ValueType_int).Enum()
		vvI = int64(v.(uint))
		break
	case uint8:
		field.V.Type = ValueType(ValueType_uint).Enum()
		vvU = uint64(v.(uint8))
		break
	case uint16:
		field.V.Type = ValueType(ValueType_uint).Enum()
		vvU = uint64(v.(uint16))
		break
	case uint32:
		field.V.Type = ValueType(ValueType_uint).Enum()
		vvU = uint64(v.(uint32))
		break
	case uint64:
		field.V.Type = ValueType(ValueType_uint).Enum()
		vvU = uint64(v.(uint64))
		break
	case float32:
		field.V.Type = ValueType(ValueType_float).Enum()
		vvF = float64(v.(float32))
		break
	case float64:
		field.V.Type = ValueType(ValueType_float).Enum()
		vvF = float64(v.(float64))
		break
	default:
		tt := reflect.TypeOf(v)
		name := tt.String()
		fmt.Println("return nil2", name)
		return nil
	}

	if field.GetType() == ValueType_float {
		field.V.F = proto.Float64(vvF)
	} else if field.GetType() == ValueType_uint {
		field.V.U = proto.Uint64(vvU)
	} else {
		field.V.I = proto.Int64(vvI)
	}
	return field
}

func PackFields(out_fields []*Field, in_fields map[string]interface{}) []*Field {
	for k, v := range in_fields {
		out_fields = append(out_fields, PackField(k, v))
	}
	return out_fields
}
