package dbmeta

import (
	"fmt"
	"github.com/sniperHW/flyfish/proto"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"unsafe"
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

type DBMeta struct {
	version     int64
	table_metas *map[string]*TableMeta
}

//根据表名获取表格元数据
func (this *DBMeta) GetTableMeta(table string) *TableMeta {
	p := (*map[string]*TableMeta)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&this.table_metas))))
	meta, ok := (*p)[table]
	if ok {
		return meta
	} else {
		return nil
	}
}

func (this *DBMeta) CheckMetaVersion(version int64) bool {
	return version == atomic.LoadInt64(&this.version)
}

//表查询元数据
type QueryMeta struct {
	field_names []string //所有的字段名
	//field_receiver []func() interface{} //用于接收查询返回值
	receiver_pool sync.Pool
	field_convter []func(interface{}) interface{}
}

func (this *QueryMeta) GetFieldNames() []string {
	return this.field_names
}

/*
func (this *QueryMeta) GetReceiverByName(name string) interface{} {
	for i := 0; i < len(this.field_names); i++ {
		if this.field_names[i] == name {
			return this.field_receiver[i]()
		}
	}
	return nil
}*/

func (this *QueryMeta) GetConvetorByName(name string) func(interface{}) interface{} {
	for i := 0; i < len(this.field_names); i++ {
		if this.field_names[i] == name {
			return this.field_convter[i]
		}
	}
	return nil
}

func (this *QueryMeta) GetFieldConvter() []func(interface{}) interface{} {
	return this.field_convter
}

func (this *QueryMeta) GetReceivers() []interface{} {
	return this.receiver_pool.Get().([]interface{})
}

func (this *QueryMeta) PutReceivers(r []interface{}) {
	this.receiver_pool.Put(r)
}

func (this *QueryMeta) getReceivers_() []interface{} {
	receiver := []interface{}{}
	for _, v := range this.field_receiver {
		receiver = append(receiver, v())
	}
	return receiver
}

//字段元信息
type FieldMeta struct {
	name     string          //字段名
	tt       proto.ValueType //字段类型
	defaultV interface{}     //字段默认值
}

func (this *FieldMeta) GetDefaultV() interface{} {
	return this.defaultV
}

//表格的元信息
type TableMeta struct {
	table            string                //表名
	fieldMetas       map[string]*FieldMeta //所有字段元信息
	queryMeta        *QueryMeta
	insertPrefix     string
	selectPrefix     string
	insertFieldOrder []string
	version          int64
}

func (this *TableMeta) GetFieldMetas() map[string]*FieldMeta {
	return this.fieldMetas
}

func (this *TableMeta) GetInsertOrder() []string {
	return this.insertFieldOrder
}

func (this *TableMeta) Version() int64 {
	return this.version
}

func (this *TableMeta) GetQueryMeta() *QueryMeta {
	return this.queryMeta
}

func (this *TableMeta) GetTable() string {
	return this.table
}

func (this *TableMeta) GetSelectPrefix() string {
	return this.selectPrefix
}

func (this *TableMeta) GetInsertPrefix() string {
	return this.insertPrefix
}

//获取字段默认值
func (this *TableMeta) GetDefaultV(name string) interface{} {
	m, ok := this.fieldMetas[name]
	if !ok {
		return nil
	} else {
		return m.defaultV
	}
}

//检查要获取的字段是否符合表配置
func (this *TableMeta) CheckGet(fields map[string]*proto.Field) error {
	for _, v := range fields {
		_, ok := this.fieldMetas[v.GetName()]
		if !ok {
			return fmt.Errorf("checkGet failed:%s", v.GetName())

		}
	}
	return nil
}

func (this *TableMeta) CheckField(field *proto.Field) error {
	m, ok := this.fieldMetas[field.GetName()]
	if !ok {
		return fmt.Errorf("checkField failed:%s", field.GetName())
	}

	if m.tt == proto.ValueType_blob {
		if !field.IsBlob() && !field.IsString() {
			return fmt.Errorf("checkField failed:%s", field.GetName())

		}
	} else if field.GetType() != m.tt {
		return fmt.Errorf("checkField failed:%s", field.GetName())
	}

	return nil
}

//检查要设置的字段是否符合表配置
func (this *TableMeta) CheckSet(fields map[string]*proto.Field) error {
	for _, v := range fields {
		m, ok := this.fieldMetas[v.GetName()]
		if !ok {
			return fmt.Errorf("checkSet failed:%s", v.GetName())
		}

		if m.tt == proto.ValueType_blob {
			if !v.IsBlob() && !v.IsString() {
				return fmt.Errorf("checkSet failed:%s", v.GetName())
			}
		} else if v.GetType() != m.tt {
			return fmt.Errorf("checkSet failed:%s", v.GetName())
		}
	}
	return nil
}

//检查要设置的新老值是否符合表配置
func (this *TableMeta) CheckCompareAndSet(newV *proto.Field, oldV *proto.Field) error {

	if newV == nil || oldV == nil {
		return fmt.Errorf("newV == nil || oldV == nil")
	}

	if newV.GetName() != oldV.GetName() {
		return fmt.Errorf("newV.GetName() != oldV.GetName()")
	}

	m, ok := this.fieldMetas[oldV.GetName()]
	if !ok {
		return fmt.Errorf("invaild filed %s", oldV.GetName())
	}

	if m.tt == proto.ValueType_blob {

		if !newV.IsBlob() && !newV.IsString() {
			return fmt.Errorf("newV is not blob:%s", newV.GetName())
		}

		if !oldV.IsBlob() && !oldV.IsString() {
			return fmt.Errorf("oldV is not blob:%s", oldV.GetName())
		}

	} else {
		if newV.GetType() != m.tt || oldV.GetType() != m.tt {
			return fmt.Errorf("newV.GetType() != m.tt || oldV.GetType() != m.tt")
		}
	}

	return nil
}

func loadMeta(def []string) (*map[string]*TableMeta, error) {
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

	table_metas := map[string]*TableMeta{}
	for _, l := range def {
		t1 := strings.Split(l, "@")

		if len(t1) != 2 {
			return nil, fmt.Errorf("len(t1) != 2 %s", l)
		}

		t_meta := &TableMeta{
			table:            t1[0],
			fieldMetas:       map[string]*FieldMeta{},
			insertFieldOrder: []string{},
			queryMeta: &QueryMeta{
				field_names:    []string{},
				field_receiver: []func() interface{}{},
				field_convter:  []func(interface{}) interface{}{},
			},
		}
		t_meta.queryMeta.receiver_pool = sync.Pool{
			New: func() interface{} {
				return t_meta.queryMeta.getReceivers_()
			},
		}

		fields := strings.Split(t1[1], ",")

		if len(fields) == 0 {
			return nil, fmt.Errorf("len(fields) == 0")
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

		//处理其它字段
		for _, v := range fields {
			if v == "" {
				break
			}

			field := strings.Split(v, ":")
			if len(field) != 3 {
				return nil, fmt.Errorf("len(fields) != 3")
			}

			name := field[0]

			//字段名不允许以__开头
			if strings.HasPrefix(name, "__") {
				return nil, fmt.Errorf("has prefix _")
			}

			ftype := getType(field[1])

			if ftype == proto.ValueType_invaild {
				return nil, fmt.Errorf("unsupport data type")
			}

			defaultValue := getDefaultV(ftype, field[2])

			if nil == defaultValue {
				return nil, fmt.Errorf("no default value")
			}

			t_meta.fieldMetas[name] = &FieldMeta{
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

	return &table_metas, nil
}

func (this *DBMeta) Reload(def []string) {
	table_metas, err := loadMeta(def)
	if nil == err {
		atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&this.table_metas)), unsafe.Pointer(table_metas))
		atomic.AddInt64(&this.version, 1)
	}
}

//tablename@field1:type:defaultValue,field2:type:defaultValue,field3:type:defaultValue...
func NewDBMeta(def []string) (*DBMeta, error) {

	table_metas, err := loadMeta(def)

	if nil == table_metas {
		return nil, err
	}

	return &DBMeta{
		version:     1,
		table_metas: table_metas,
	}, nil
}
