package proto

func ToValue(v interface{}) (vv *Value) {
	vv = &Value{}
	if nil == v {
		vv.Type = ValueType_nil
		return
	}

	switch v.(type) {
	case []byte:
		vv.Type = ValueType_blob
		vv.Blob = v.([]byte)
	case string:
		vv.Type = ValueType_string
		vv.String_ = v.(string)
	case int:
		vv.Type = ValueType_int
		vv.Int = int64(v.(int))
	case uint:
		vv.Type = ValueType_int
		vv.Int = int64(v.(uint))
	case int8:
		vv.Type = ValueType_int
		vv.Int = int64(v.(int8))
	case uint8:
		vv.Type = ValueType_int
		vv.Int = int64(v.(uint8))
	case int16:
		vv.Type = ValueType_int
		vv.Int = int64(v.(int16))
	case uint16:
		vv.Type = ValueType_int
		vv.Int = int64(v.(uint16))
	case int32:
		vv.Type = ValueType_int
		vv.Int = int64(v.(int32))
	case uint32:
		vv.Type = ValueType_int
		vv.Int = int64(v.(uint32))
	case int64:
		vv.Type = ValueType_int
		vv.Int = int64(v.(int64))
	case uint64:
		vv.Type = ValueType_int
		vv.Int = int64(v.(uint64))
	case float32:
		vv.Type = ValueType_float
		vv.Float = float64(v.(float32))
	case float64:
		vv.Type = ValueType_float
		vv.Float = float64(v.(float64))
	default:
		vv.Type = ValueType_invaild
	}

	return
}

func (v *Value) GetString() string {
	return v.GetString_()
}

func (v *Value) IsEqual(o *Value) bool {
	if v.GetType() != o.GetType() {
		return false
	}

	switch v.GetType() {
	case ValueType_string:
		return v.GetString() == o.GetString()
	case ValueType_float:
		return v.GetFloat() == o.GetFloat()
	case ValueType_int:
		return v.GetInt() == o.GetInt()
	case ValueType_blob:
		mm := v.GetBlob()
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

func (m *Field) GetType() ValueType {
	return m.V.GetType()
}

func (m *Field) IsNil() bool {
	return m.V.GetType() == ValueType_nil
}

func (m *Field) IsString() bool {
	return m.V.GetType() == ValueType_string
}

func (m *Field) IsInt() bool {
	return m.V.GetType() == ValueType_int
}

func (m *Field) IsFloat() bool {
	return m.V.GetType() == ValueType_float
}

func (m *Field) IsBlob() bool {
	return m.V.GetType() == ValueType_blob
}

func (m *Field) GetValue() interface{} {
	switch m.V.GetType() {
	case ValueType_string:
		return m.V.GetString()
	case ValueType_int:
		return m.V.GetInt()
	case ValueType_float:
		return m.V.GetFloat()
	case ValueType_blob:
		return m.V.GetBlob()
	default:
		return nil
	}
}

func (m *Field) GetValueReceiver() interface{} {
	switch m.V.GetType() {
	case ValueType_string:
		return new(string)
	case ValueType_int:
		return new(int64)
	case ValueType_float:
		return new(float64)
	case ValueType_blob:
		return new([]byte)
	default:
		return nil
	}
}

func convert_string(in interface{}) interface{} {
	return *(in.(*string))
}

func convert_int64(in interface{}) interface{} {
	return *(in.(*int64))
}

func convert_float(in interface{}) interface{} {
	return *(in.(*float64))
}

func convert_blob(in interface{}) interface{} {
	return *in.(*[]byte)
}

func (m *Field) GetValueConvtor() func(interface{}) interface{} {
	switch m.V.GetType() {
	case ValueType_string:
		return convert_string
	case ValueType_int:
		return convert_int64
	case ValueType_float:
		return convert_float
	case ValueType_blob:
		return convert_blob
	default:
		return nil
	}
}

func (m *Field) GetString() string {
	return m.V.GetString()
}

func (m *Field) GetBlob() []byte {
	return m.V.GetBlob()
}

func (m *Field) GetInt() int64 {
	return m.V.GetInt()
}

func (m *Field) GetFloat() float64 {
	return m.V.GetFloat()
}

func (m *Field) IsEqual(o *Field) bool {
	return m.V.IsEqual(o.V)
}

func PackField(name string, v interface{}) *Field {
	return &Field{
		Name: name,
		V:    ToValue(v),
	}
}

func PackFields(out_fields []*Field, in_fields map[string]interface{}) []*Field {
	for k, v := range in_fields {
		out_fields = append(out_fields, PackField(k, v))
	}
	return out_fields
}
