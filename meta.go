package flyfish

import (
	"flyfish/proto"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"unsafe"
)

var (
	meta_version  int64
	g_table_metas *map[string]*table_meta
)

func convert_string(in interface{}) interface{} {
	return *(in.(*string))
}

func convert_int64(in interface{}) interface{} {
	return *(in.(*int64))
}

func convert_uint64(in interface{}) interface{} {
	return *(in.(*uint64))
}

func convert_float(in interface{}) interface{} {
	return *(in.(*float64))
}

func convert_blob(in interface{}) interface{} {
	return *in.(*[]byte)
}

//表查询元数据
type query_meta struct {
	field_names    []string             //所有的字段名
	field_receiver []func() interface{} //用于接收查询返回值
	receiver_pool  sync.Pool
	field_convter  []func(interface{}) interface{}
}

func (this *query_meta) getReceiverByName(name string) interface{} {
	for i := 0; i < len(this.field_names); i++ {
		if this.field_names[i] == name {
			return this.field_receiver[i]()
		}
	}
	return nil
}

func (this *query_meta) getConvetorByName(name string) func(interface{}) interface{} {
	for i := 0; i < len(this.field_names); i++ {
		if this.field_names[i] == name {
			return this.field_convter[i]
		}
	}
	return nil
}

func (this *query_meta) getReceiver() []interface{} {
	return this.receiver_pool.Get().([]interface{})
}

func (this *query_meta) putReceiver(r []interface{}) {
	this.receiver_pool.Put(r)
}

func (this *query_meta) getReceiver_() []interface{} {
	receiver := []interface{}{}
	for _, v := range this.field_receiver {
		receiver = append(receiver, v())
	}
	return receiver
}

//字段元信息
type field_meta struct {
	name     string
	tt       proto.ValueType
	defaultV interface{}
}

//表格的元信息
type table_meta struct {
	table            string
	fieldMetas       map[string]field_meta
	queryMeta        query_meta
	insertPrefix     string
	selectPrefix     string
	insertFieldOrder []string
	meta_version     int64
}

//获取字段默认值
func (this *table_meta) getDefaultV(name string) interface{} {
	m, ok := this.fieldMetas[name]
	if !ok {
		return nil
	} else {
		return m.defaultV
	}
}

//检查要获取的字段是否符合表配置
func (this *table_meta) checkGet(fields map[string]*proto.Field) bool {
	for _, v := range fields {
		_, ok := this.fieldMetas[v.GetName()]
		if !ok {
			Errorln("checkGet failed:", v.GetName())
			return false
		}
	}
	return true
}

func (this *table_meta) checkField(field *proto.Field) bool {
	m, ok := this.fieldMetas[field.GetName()]
	if !ok {
		Errorln("checkField failed:", field.GetName())
		return false
	}

	if field.GetType() != m.tt {
		Errorln("checkField failed:", field.GetName(), field.GetType())
		return false
	}

	return true

}

//检查要设置的字段是否符合表配置
func (this *table_meta) checkSet(fields map[string]*proto.Field) bool {
	for _, v := range fields {
		m, ok := this.fieldMetas[v.GetName()]
		if !ok {
			Errorln("checkSet failed:", v.GetName())
			return false
		}

		if v.GetType() != m.tt {
			Errorln("checkSet failed:", v.GetName(), v.GetType())
			return false
		}
	}
	return true
}

//检查要设置的新老值是否符合表配置
func (this *table_meta) checkCompareAndSet(newV *proto.Field, oldV *proto.Field) bool {

	if newV == nil || oldV == nil {
		return false
	}

	if newV.GetType() != oldV.GetType() {
		return false
	}

	if newV.GetName() != oldV.GetName() {
		return false
	}

	m, ok := this.fieldMetas[oldV.GetName()]
	if !ok {
		return false
	}

	if oldV.GetType() != m.tt {
		return false
	}

	return true
}

//根据表名获取表格元数据
func getMetaByTable(table string) *table_meta {
	p := (*map[string]*table_meta)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&g_table_metas))))
	meta, ok := (*p)[table]
	if ok {
		return meta
	} else {
		return nil
	}
}

func checkMetaVersion(version int64) bool {
	return version == atomic.LoadInt64(&meta_version)
}

//tablename@field1:type:defaultValue,field2:type:defaultValue,field3:type:defaultValue...
func LoadMeta(def []string) bool {

	getType := func(str string) proto.ValueType {
		if str == "int" {
			return proto.ValueType_int
		} else if str == "uint" {
			return proto.ValueType_uint
		} else if str == "float" {
			return proto.ValueType_float
		} else if str == "string" {
			return proto.ValueType_string
		} else if str == "blob" {
			return proto.ValueType_blob
		} else {
			return proto.ValueType_invaild
		}
	}

	getReceiver := func(tt proto.ValueType) interface{} {
		if tt == proto.ValueType_int {
			return new(int64)
		} else if tt == proto.ValueType_uint {
			return new(uint64)
		} else if tt == proto.ValueType_float {
			return new(float64)
		} else if tt == proto.ValueType_string {
			return new(string)
		} else if tt == proto.ValueType_blob {
			b := []byte{}
			return &b
		} else {
			return nil
		}
	}

	getConvetor := func(tt proto.ValueType) func(interface{}) interface{} {
		if tt == proto.ValueType_int {
			return convert_int64
		} else if tt == proto.ValueType_uint {
			return convert_uint64
		} else if tt == proto.ValueType_float {
			return convert_float
		} else if tt == proto.ValueType_string {
			return convert_string
		} else if tt == proto.ValueType_blob {
			return convert_blob
		} else {
			return nil
		}
	}

	getDefaultV := func(tt proto.ValueType, v string) interface{} {
		if tt == proto.ValueType_string {
			return v
		} else if tt == proto.ValueType_int {
			if v == "" {
				return int64(0)
			} else {
				i, err := strconv.ParseInt(v, 10, 64)
				if nil != err {
					return nil
				} else {
					return i
				}
			}
		} else if tt == proto.ValueType_uint {
			if v == "" {
				return uint64(0)
			} else {
				u, err := strconv.ParseUint(v, 10, 64)
				if nil != err {
					return nil
				} else {
					return u
				}
			}
		} else if tt == proto.ValueType_float {
			if v == "" {
				return float64(0)
			} else {
				f, err := strconv.ParseFloat(v, 64)
				if nil != err {
					return nil
				} else {
					return f
				}
			}
		} else if tt == proto.ValueType_blob {
			return []byte{}
		} else {
			return nil
		}

	}

	table_metas := map[string]*table_meta{}
	for _, l := range def {
		t1 := strings.Split(l, "@")

		if len(t1) != 2 {
			return false
		}

		t_meta := &table_meta{
			table:            t1[0],
			fieldMetas:       map[string]field_meta{},
			insertFieldOrder: []string{},
			queryMeta: query_meta{
				field_names:    []string{},
				field_receiver: []func() interface{}{},
				field_convter:  []func(interface{}) interface{}{},
			},
		}
		t_meta.queryMeta.receiver_pool = sync.Pool{
			New: func() interface{} {
				return t_meta.queryMeta.getReceiver_()
			},
		}

		fields := strings.Split(t1[1], ",")

		if len(fields) == 0 {
			return false
		}

		//加上__key__，__version__
		if len(fields)+2 > len(ARGV)-1 {
			Errorln("len(fields)+2 > len(ARGV)-1")
			return false
		}

		//插入两个默认字段
		t_meta.queryMeta.field_names = append(t_meta.queryMeta.field_names, "__key__")
		t_meta.queryMeta.field_receiver = append(t_meta.queryMeta.field_receiver, func() interface{} {
			return getReceiver(proto.ValueType_string)
		})
		t_meta.queryMeta.field_convter = append(t_meta.queryMeta.field_convter, getConvetor(proto.ValueType_string))

		t_meta.queryMeta.field_names = append(t_meta.queryMeta.field_names, "__version__")
		t_meta.queryMeta.field_receiver = append(t_meta.queryMeta.field_receiver, func() interface{} {
			return getReceiver(proto.ValueType_int)
		})
		t_meta.queryMeta.field_convter = append(t_meta.queryMeta.field_convter, getConvetor(proto.ValueType_int))

		//fieldNames := []string{}
		//处理其它字段
		for _, v := range fields {
			field := strings.Split(v, ":")
			if len(field) != 3 {
				return false
			}

			name := field[0]

			//字段名不允许以__开头
			if strings.HasPrefix(name, "__") {
				return false
			}

			ftype := getType(field[1])

			if ftype == proto.ValueType_invaild {
				return false
			}

			defaultValue := getDefaultV(ftype, field[2])

			if nil == defaultValue {
				return false
			}

			t_meta.fieldMetas[name] = field_meta{
				name:     name,
				tt:       ftype,
				defaultV: defaultValue,
			}

			t_meta.insertFieldOrder = append(t_meta.insertFieldOrder, name)

			t_meta.queryMeta.field_names = append(t_meta.queryMeta.field_names, name)

			t_meta.queryMeta.field_receiver = append(t_meta.queryMeta.field_receiver, func() interface{} {
				return getReceiver(ftype)
			})

			t_meta.queryMeta.field_convter = append(t_meta.queryMeta.field_convter, getConvetor(ftype))

			table_metas[t1[0]] = t_meta
		}

		t_meta.selectPrefix = fmt.Sprintf("SELECT %s FROM %s where __key__ in(", strings.Join(t_meta.queryMeta.field_names, ","), t_meta.table)
		t_meta.insertPrefix = fmt.Sprintf("INSERT INTO %s(__key__,__version__,%s) VALUES (", t_meta.table, strings.Join(t_meta.insertFieldOrder, ","))

	}

	atomic.AddInt64(&meta_version, 1)
	atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&g_table_metas)), unsafe.Pointer(&table_metas))

	Infoln("load table meta ok,version:", meta_version)

	return true

}
