package flyfish

import (
	protocol "flyfish/proto"
	"strings"
	"sync"
	"strconv"
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
	field_names     []string                                      //所有的字段名
	field_receiver  []func()interface{}                           //用于接收查询返回值
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


type field_meta struct {
	name      string
	tt        protocol.ValueType
	defaultV  interface{}
}

//表的元数据
type table_meta struct {
	table       string 
	fieldMetas  map[string]field_meta
	queryMeta	query_meta
}

/*
func (this *table_meta) fillDefault(out *map[string]*protocol.Field) {
	for _ ,v := range(this.fieldMetas) {
		(*out)[v.name] = protocol.PackField(v.name,v.defaultV)
	}
}
*/

func (this *table_meta) getDefaultV(name string) interface{} {
	m,ok := this.fieldMetas[name]
	if !ok {
		return nil
	} else {
		return m.defaultV
	}
}

//检查要获取的字段是否符合表配置
func (this *table_meta) checkGet(fields map[string]*protocol.Field) bool {
	for _,v := range(fields) {
		_,ok := this.fieldMetas[v.GetName()]
		if !ok {
			return false
		}
	}
	return true
}


func (this *table_meta) checkField(field *protocol.Field) bool {
	m,ok := this.fieldMetas[field.GetName()]
	if !ok {
		return false
	}

	if field.GetType() != m.tt {
		return false
	}

	return true

}

//检查要设置的字段是否符合表配置
func (this *table_meta) checkSet(fields map[string]*protocol.Field) bool {
	for _,v := range(fields) {
		m,ok := this.fieldMetas[v.GetName()]
		if !ok {
			return false
		}

		if v.GetType() != m.tt {
			return false
		} 
	}
	return true
}

//检查要设置的新老值是否符合表配置
func (this *table_meta) checkCompareAndSet(newV *protocol.Field,oldV *protocol.Field) bool {

	if newV == nil || oldV == nil {
		return false
	}

	if newV.GetType() != oldV.GetType() {
		return false
	}

	if newV.GetName() != oldV.GetName() {
		return false
	}


	m,ok := this.fieldMetas[oldV.GetName()]
	if !ok {
		return false
	}

	if oldV.GetType() != m.tt {
		return false
	}

	return true
}

func getMetaByTable(table string) *table_meta {
	meta,ok := table_metas[table]
	if ok {
		return meta
	} else {
		return nil
	}
}

//tablename@field1:type:defaultValue,field2:type:defaultValue,field3:type:defaultValue...
func InitMeta(def []string) bool {

	getType := func(str string) protocol.ValueType {
		if str == "int" {
			return protocol.ValueType_int
		} else if str == "uint" {
			return protocol.ValueType_uint			
		} else if str == "float" {
			return protocol.ValueType_float		
		} else if str == "string" {
			return protocol.ValueType_string			
		} else {
			return protocol.ValueType_invaild
		}
	}

	getReceiver := func(tt protocol.ValueType) interface{} {
		if tt == protocol.ValueType_int {
			return new(int64)
		} else if tt == protocol.ValueType_uint {
			return new(uint64)
		} else if tt == protocol.ValueType_float {
			return new(float64)
		} else if tt == protocol.ValueType_string {
			return new(string)
		} else {
			return nil
		}
	}

	getConvetor := func(tt protocol.ValueType) func(interface{})interface{} {
		if tt == protocol.ValueType_int {
			return convter_int64
		} else if tt == protocol.ValueType_uint {
			return convter_uint64
		} else if tt == protocol.ValueType_float {
			return convter_float
		} else if tt == protocol.ValueType_string {
			return convter_string
		} else {
			return nil
		}		
	}

	getDefaultV := func(tt protocol.ValueType,v string) interface{} {
		if tt == protocol.ValueType_string {
			return v
		} else if tt == protocol.ValueType_int {
			i,err := strconv.ParseInt(v,10,64)
			if nil != err {
				return nil
			} else {
				return i
			}
		} else if tt == protocol.ValueType_uint {
			u,err := strconv.ParseUint(v,10,64)
			if nil != err {
				return nil
			} else {
				return u
			}
		} else if tt == protocol.ValueType_float {
			f,err := strconv.ParseFloat(v,64)
			if nil != err {
				return nil
			} else {
				return f
			}
		} else {
			return nil
		}
		
	} 

	table_metas  = map[string]*table_meta{}
	for _,l := range(def) {
		t1 := strings.Split(l,"@")

		if len(t1) != 2 {
			return false
		}

		t_meta := &table_meta {
			table  : t1[0],
			fieldMetas : map[string]field_meta{},
			queryMeta : query_meta {
				field_names    : []string{},
				field_receiver : []func()interface{}{},
				field_convter  : []func(interface{})interface{}{},
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

		//插入两个默认字段
		t_meta.queryMeta.field_names = append(t_meta.queryMeta.field_names,"__key__")
		t_meta.queryMeta.field_receiver = append(t_meta.queryMeta.field_receiver,func() interface{} {
		 	return getReceiver(protocol.ValueType_string)
		})
		t_meta.queryMeta.field_convter = append(t_meta.queryMeta.field_convter,getConvetor(protocol.ValueType_string))

		t_meta.queryMeta.field_names = append(t_meta.queryMeta.field_names,"__version__")
		t_meta.queryMeta.field_receiver = append(t_meta.queryMeta.field_receiver,func() interface{} {
			return getReceiver(protocol.ValueType_int)
		})
		t_meta.queryMeta.field_convter = append(t_meta.queryMeta.field_convter,getConvetor(protocol.ValueType_int))

		//处理其它字段
		for _,v := range(fields) {
			field := strings.Split(v,":")
			if len(field) != 3 {
				return false
			}

			name := field[0]

			//字段名不允许以__开头
			if strings.HasPrefix(name,"__") {
				return false
			}

			ftype := getType(field[1])

			if ftype == protocol.ValueType_invaild {
				return false
			}

			defaultValue := getDefaultV(ftype,field[2])

			if nil == defaultValue {
				return false
			}

			t_meta.fieldMetas[name] = field_meta {
				name : name,
				tt   : ftype,
				defaultV : defaultValue,
			}

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



 
