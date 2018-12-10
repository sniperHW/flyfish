package proto

import (
	"fmt"
	"reflect"
	"github.com/golang/protobuf/proto"
	"strconv"
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

func (m *Field) GetValue() interface{} {
	return UnpackField(m)
}

func (m *Field) GetString() string {

	if !m.IsString() {
		panic("v is not string")
	}

	return m.V.GetS()
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

func (m *Field) ToSqlStr() string {
	switch m.GetType() {
		case ValueType_string:
			return fmt.Sprintf("'%s'",m.GetString()) 
		case ValueType_float:
			return fmt.Sprintf("%f",m.GetFloat())
		case ValueType_int:
			return strconv.FormatInt(m.GetInt(),10)
		case ValueType_uint:
			return strconv.FormatUint(m.GetUint(),10)
		default:
			panic("invaild value type")
	}
	return ""
}

func (m *Field) Equal(o *Field) bool {
	if nil == o {
		return false
	} else if m.GetType() != o.GetType() {
		return false
	}

	tt := m.GetType()

	if tt == ValueType_string {
		return m.GetString() == o.GetString()
	} else if tt == ValueType_float {
		return m.GetFloat() == o.GetFloat()		
	} else if tt == ValueType_int {
		return m.GetInt() == o.GetInt()
	} else if tt == ValueType_uint {
		return m.GetUint() == o.GetUint()
	} else {
		return true
	}
}

func UnpackField(filed *Field) interface{} {
	if nil == filed {
		return nil
	}

	if filed.IsNil() {
		return nil
	} else if filed.IsString() {
		return filed.GetString()
	} else if filed.IsInt() {
		return filed.GetInt()
	} else if filed.IsUint() {
		return filed.GetUint()
	} else if filed.IsFloat() {
		return filed.GetFloat()
	} else {
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
		case int:
		case int8:
		case int16:
		case int32:
		case int64:
			return ValueType_int	
		case uint:	
		case uint8:	
		case uint16:
		case uint32:				
		case uint64:
			return ValueType_uint
		case float32:
		case float64:
			return ValueType_float
		default:
			return ValueType_invaild
	}
	return ValueType_invaild
}


func PackField(name string,v interface{}) *Field {

	field := &Field{
		V    : &Value{},
		Name : proto.String(name),
	}

	if nil == v {
		field.V.Type = ValueType(ValueType_nil).Enum()
		return field
	}

	var vvI  int64
	var vvF  float64
	var vvU  uint64

	switch v.(type) {
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
			fmt.Println("return nil2",name)
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


func PackFields(out_fields []*Field,in_fields map[string]interface{}) []*Field {
	for k,v := range(in_fields) {
		out_fields = append(out_fields,PackField(k,v))
	}
	return out_fields
}