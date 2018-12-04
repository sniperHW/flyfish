package flyfish

import (
	message "flyfish/proto"
	"strings"
	"sync"
)

var table_metas map[string]*table_meta

func convter_string(in interface{}) interface{} {
	return *(in.(*string))
}

func convter_int64(in interface{}) interface{} {
	return *(in.(*int64))
}

func convter_uint64(in interface{}) interface{} {
	return *(in.(*uint64))
}

func convter_float(in interface{}) interface{} {
	return *(in.(*float64))
}

//表查询元数据
type query_meta struct {
	field_names     []string                   //所有的字段名
	field_receiver  []func()interface{}
	receiver_pool   sync.Pool
	field_convter   []func(interface{})interface{}
}

func (this *query_meta) getReceiver() []interface{} {
	return this.receiver_pool.Get().([]interface{})
}

func (this *query_meta) putReceiver(r []interface{}) {
	this.receiver_pool.Put(r)
}

func (this *query_meta) getReceiver_() []interface{} {
	receiver := []interface{}{}
	for _,v := range(this.field_receiver) {
		receiver = append(receiver,v())
	}
	return receiver
}

//表的元数据
type table_meta struct {
	table       string 
	field_types map[string]message.ValueType    //各字段类型
	queryMeta	query_meta
}

func (this *table_meta) checkGet(fields []field) bool {
	for _,v := range(fields) {
		_,ok := this.field_types[v.name]
		if !ok {
			return false
		}
	}
	return true
}

func (this *table_meta) checkSet(fields []field) bool {
	for _,v := range(fields) {
		m,ok := this.field_types[v.name]
		if !ok {
			return false
		}

		if v.Tt() != m {
			return false
		} 
	}
	return true
}

func (this *table_meta) checkCompareAndSet(oldV *field,newV *field) bool {
	var ok bool
	var m  message.ValueType
	m,ok = this.field_types[oldV.name]
	if !ok {
		return false
	}

	if oldV.Tt() != m {
		return false
	}

	m,ok = this.field_types[newV.name]
	if !ok {
		return false
	}

	if newV.Tt() != m {
		return false
	}

	return true	
}

func GetMetaByTable(table string) *table_meta {
	meta,ok := table_metas[table]
	if ok {
		return meta
	} else {
		return nil
	}
}

//tablename@field1:type,field2:type,field3:type...
func InitMeta(metas []string) bool {

	getType := func(str string) message.ValueType {
		if str == "int" {
			return message.ValueType_Integer
		} else if str == "uint" {
			return message.ValueType_Uinteger			
		} else if str == "float" {
			return message.ValueType_Float			
		} else if str == "string" {
			return message.ValueType_String			
		} else {
			return message.ValueType_Invaild
		}
	}

	getReceiver := func(tt message.ValueType) interface{} {
		if tt == message.ValueType_Integer {
			return new(int64)
		} else if tt == message.ValueType_Uinteger {
			return new(uint64)
		} else if tt == message.ValueType_Float {
			return new(float64)
		} else if tt == message.ValueType_String {
			return new(string)
		} else {
			return nil
		}
	}

	getConvetor := func(tt message.ValueType) func(interface{})interface{} {
		if tt == message.ValueType_Integer {
			return convter_int64
		} else if tt == message.ValueType_Uinteger {
			return convter_uint64
		} else if tt == message.ValueType_Float {
			return convter_float
		} else if tt == message.ValueType_String {
			return convter_string
		} else {
			return nil
		}		
	} 

	table_metas  = map[string]*table_meta{}
	for _,meta := range(metas) {
		t1 := strings.Split(meta,"@")

		if len(t1) != 2 {
			return false
		}

		t_meta := &table_meta {
			table  : t1[0],
			field_types : map[string]message.ValueType{},
			queryMeta : query_meta {
				field_names : []string{},
				field_receiver : []func()interface{}{},
				field_convter : []func(interface{})interface{}{},
			},
		}
		t_meta.queryMeta.receiver_pool = sync.Pool {
			New : func() interface{} {
				return t_meta.queryMeta.getReceiver_()
			},
		}

		fields := strings.Split(t1[1],",")

		if len(fields) == 0 {
			return false
		}

		t_meta.queryMeta.field_names = append(t_meta.queryMeta.field_names,"__key__")
		t_meta.queryMeta.field_receiver = append(t_meta.queryMeta.field_receiver,func() interface{} {
		 	return getReceiver(message.ValueType_String)
		})
		t_meta.queryMeta.field_convter = append(t_meta.queryMeta.field_convter,getConvetor(message.ValueType_String))

		t_meta.queryMeta.field_names = append(t_meta.queryMeta.field_names,"__version__")
		t_meta.queryMeta.field_receiver = append(t_meta.queryMeta.field_receiver,func() interface{} {
			return getReceiver(message.ValueType_Integer)
		})
		t_meta.queryMeta.field_convter = append(t_meta.queryMeta.field_convter,getConvetor(message.ValueType_Integer))

		for _,v := range(fields) {
			field := strings.Split(v,":")
			if len(field) != 2 {
				return false
			}

			name := field[0]
			ftype := getType(field[1])

			if ftype == message.ValueType_Invaild {
				return false
			}

			t_meta.field_types[name] = ftype
			t_meta.queryMeta.field_names = append(t_meta.queryMeta.field_names,name)
			t_meta.queryMeta.field_receiver = append(t_meta.queryMeta.field_receiver,func() interface{} {
			 	return getReceiver(ftype)
			})
			t_meta.queryMeta.field_convter = append(t_meta.queryMeta.field_convter,getConvetor(ftype))			

			table_metas[t1[0]] = t_meta
		}
	}

	return true

}



 
