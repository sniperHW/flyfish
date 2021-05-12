package proto

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
	m.V.I = v
}

func (m *Field) SetString(v string) {
	if !m.IsString() {
		panic("v is not string")
	}
	m.V.S = v
}

func (m *Field) SetFloat(v float64) {
	if !m.IsFloat() {
		panic("v is not float")
	}
	m.V.F = v
}

func (m *Field) SetBlob(v []byte) {
	if !m.IsBlob() {
		panic("v is not Blob")
	}
	m.V.B = v
}

func (m *Field) IsEqual(o *Field) bool {
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
	case ValueType_float:
		return filed.GetFloat()
	case ValueType_blob:
		return filed.GetBlob()
	default:
		return nil
	}
}

func PackField(name string, v interface{}) *Field {

	field := &Field{
		V:    &Value{},
		Name: name,
	}

	if nil == v {
		field.V.Type = ValueType_nil
		return field
	}

	var vvI int64
	var vvF float64

	switch v.(type) {
	case []byte:
		field.V.B = v.([]byte)
		field.V.Type = ValueType_blob
		return field
	case string:
		field.V.S = v.(string)
		field.V.Type = ValueType_string
		return field
	case int:
		field.V.Type = ValueType_int
		vvI = int64(v.(int))
		break
	case int8:
		field.V.Type = ValueType_int
		vvI = int64(v.(int8))
		break
	case int16:
		field.V.Type = ValueType_int
		vvI = int64(v.(int16))
		break
	case int32:
		field.V.Type = ValueType_int
		vvI = int64(v.(int32))
		break
	case int64:
		field.V.Type = ValueType_int
		vvI = int64(v.(int64))
		break
	case float32:
		field.V.Type = ValueType_float
		vvF = float64(v.(float32))
		break
	case float64:
		field.V.Type = ValueType_float
		vvF = float64(v.(float64))
		break
	default:
		return nil
	}

	if field.GetType() == ValueType_float {
		field.V.F = vvF
	} else {
		field.V.I = vvI
	}
	return field
}

func PackFields(out_fields []*Field, in_fields map[string]interface{}) []*Field {
	for k, v := range in_fields {
		out_fields = append(out_fields, PackField(k, v))
	}
	return out_fields
}
