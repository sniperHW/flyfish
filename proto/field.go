package proto

import (
	"fmt"
	"reflect"
	"github.com/golang/protobuf/proto"
	//"strconv"
)

func (this *Field) GetTt() ValueType {
	if this.GetV() == nil {
		return ValueType_Nil
	}
	return this.V.GetTt()
}

func (this *Field) IsNil() bool {
	if this.GetTt() == ValueType_Nil || this.GetV() == nil {
		return true
	}
	return false
}

func (this *Field) GetString() string {
	if this.IsNil() {
		panic("v is nil")
	}

	if this.GetTt() != ValueType_String {
		panic("v is not string")
	}

	return this.GetV().GetVString()
}

func (this *Field) GetUint() uint64 {

	if this.IsNil() {
		panic("v is nil")
	}

	if this.GetTt() != ValueType_Uinteger {
		panic("v is not uinteger")
	}

	return this.GetV().GetVUInt()	
}

func (this *Field) GetInt() int64 {
	if this.IsNil() {
		panic("v is nil")
	}

	if this.GetTt() != ValueType_Integer {
		panic("v is not integer")
	}

	return this.GetV().GetVInt()
}

func (this *Field) GetFloat() float64 {
	if this.IsNil() {
		panic("v is nil")
	}

	if this.GetTt() != ValueType_Float {
		panic("v is not float")
	}

	return this.GetV().GetVFloat()
}

func UnpackField(filed *Field) interface{} {
	if nil == filed {
		return nil
	}

	if nil == filed.V {
		return nil
	}

	tt := filed.V.GetTt()

	if tt == ValueType_Nil {
		return nil
	} else if tt == ValueType_Float {
		return filed.GetFloat()
	} else if tt == ValueType_Integer {
		return filed.GetInt()
	} else if tt == ValueType_Uinteger {
		return filed.GetUint()
	} else {
		return filed.GetString()
	}
}


func GetValueType(v interface{}) ValueType {
	if nil == v {
		return ValueType_Nil
	}

	switch v.(type) {
		case string:
			return ValueType_String
		case int:
		case int8:
		case int16:
		case int32:
		case int64:
			return ValueType_Integer	
		case uint:	
		case uint8:	
		case uint16:
		case uint32:				
		case uint64:
			return ValueType_Uinteger
		case float32:
		case float64:
			return ValueType_Uinteger
		default:
			return ValueType_Invaild
	}
	return ValueType_Invaild
}


func PackField(name string,v interface{}) *Field {

	field := &Field{
		V    : &Value{},
		Name : proto.String(name),
	}

	if nil == v {
		field.V.Tt = ValueType(ValueType_Nil).Enum()
		return field
	}

	var vvI  int64
	var vvF  float64
	var vvU  uint64

	switch v.(type) {
		case string:
			field.V.VString = proto.String(v.(string))
			field.V.Tt = ValueType(ValueType_String).Enum()
			return field
		case int:
			field.V.Tt = ValueType(ValueType_Integer).Enum()
			vvI = int64(v.(int))
			break
		case int8:
			field.V.Tt = ValueType(ValueType_Integer).Enum()
			vvI = int64(v.(int8))
			break
		case int16:
			field.V.Tt = ValueType(ValueType_Integer).Enum()
			vvI = int64(v.(int16))
			break	
		case int32:
			field.V.Tt = ValueType(ValueType_Integer).Enum()
			vvI = int64(v.(int32))
			break
		case int64:
			field.V.Tt = ValueType(ValueType_Integer).Enum()
			vvI = int64(v.(int64))
			break		
		case uint:
			field.V.Tt = ValueType(ValueType_Integer).Enum()
			vvI = int64(v.(uint))
			break			
		case uint8:
			field.V.Tt = ValueType(ValueType_Uinteger).Enum()
			vvU = uint64(v.(uint8))
			break		
		case uint16:
			field.V.Tt = ValueType(ValueType_Uinteger).Enum()
			vvU = uint64(v.(uint16))
			break
		case uint32:
			field.V.Tt = ValueType(ValueType_Uinteger).Enum()
			vvU = uint64(v.(uint32))
			break					
		case uint64:
			field.V.Tt = ValueType(ValueType_Uinteger).Enum()
			vvU = uint64(v.(uint64))
			break
		case float32:
			field.V.Tt = ValueType(ValueType_Float).Enum()
			vvF = float64(v.(float32))
			break
		case float64:
			field.V.Tt = ValueType(ValueType_Float).Enum()
			vvF = float64(v.(float64))
			break
		default:
			tt := reflect.TypeOf(v)
			name := tt.String()
			fmt.Println("return nil2",name)
			return nil
	}

	if field.V.GetTt() == ValueType_Float {
		field.V.VFloat = proto.Float64(vvF)
	} else if field.V.GetTt() == ValueType_Uinteger {
		field.V.VUInt = proto.Uint64(vvU)
	} else {
		field.V.VInt = proto.Int64(vvI)
	}
	return field	
}


func PackFields(out_fields []*Field,in_fields map[string]interface{}) []*Field {
	for k,v := range(in_fields) {
		out_fields = append(out_fields,PackField(k,v))
	}
	return out_fields
}

/*
func PackFields(fields []*Field,names []string,result []interface{}) []*Field {
	if nil != names && len(names) == len(result) {
		for i,v := range(result) {
			fields = append(fields,PackField(&names[i],v))
		}
	} else {
		for _,v := range(result) {
			fields = append(fields,PackField(nil,v))
		}
	}
	return fields
}
*/