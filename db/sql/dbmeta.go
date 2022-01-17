package sql

import (
	"errors"
	"fmt"
	"github.com/sniperHW/flyfish/db"
	"github.com/sniperHW/flyfish/proto"
	"reflect"
	"strings"
	"sync"
)

//表查询元数据
type QueryMeta struct {
	field_names      []string //所有的字段名
	real_field_names []string
	field_convter    []func(interface{}) interface{}
	field_type       []reflect.Type
}

func (this *QueryMeta) GetFieldNames() []string {
	return this.field_names
}

func (this *QueryMeta) GetConvetorByName(name string) func(interface{}) interface{} {
	for i := 0; i < len(this.field_names); i++ {
		if this.field_names[i] == name {
			return this.field_convter[i+3]
		}
	}
	return nil
}

func (this *QueryMeta) GetFieldConvter() []func(interface{}) interface{} {
	return this.field_convter
}

func (this *QueryMeta) GetReceiver() []interface{} {
	receiver := make([]interface{}, len(this.field_type))
	for i, v := range this.field_type {
		receiver[i] = reflect.New(v).Interface()
	}
	return receiver
}

//字段元信息
type FieldMeta struct {
	tabVersion   int64
	name         string          //字段名
	tt           proto.ValueType //字段类型
	defaultValue interface{}     //字段默认值
}

func (this *FieldMeta) GetDefaultValue() interface{} {
	return this.defaultValue
}

func (this *FieldMeta) getRealName() string {
	return fmt.Sprintf("%s_%d", this.name, this.tabVersion)
}

type DBMeta struct {
	sync.RWMutex
	tables map[string]*TableMeta
	def    *db.DbDef
}

func (this *DBMeta) GetDef() *db.DbDef {
	this.RLock()
	defer this.RUnlock()
	return this.def
}

func (this *DBMeta) ToJson() ([]byte, error) {
	return this.def.ToJson()
}

func (this *DBMeta) ToPrettyJson() ([]byte, error) {
	return this.def.ToPrettyJson()
}

func (this *DBMeta) GetTableMeta(tab string) db.TableMeta {
	this.RLock()
	defer this.RUnlock()
	if v, ok := this.tables[tab]; ok {
		return v
	} else {
		return nil
	}
}

func (this *DBMeta) GetVersion() int64 {
	this.RLock()
	defer this.RUnlock()
	return this.def.Version
}

func (this *DBMeta) MoveTo(other db.DBMeta) {
	this.Lock()
	defer this.Unlock()
	o := other.(*DBMeta)
	o.Lock()
	defer o.Unlock()
	if nil != this.tables {
		o.def = this.def
		o.tables = this.tables
		this.def = nil
		this.tables = nil
	}
}

func (this *DBMeta) CheckTableMeta(tab db.TableMeta) db.TableMeta {
	this.RLock()
	defer this.RUnlock()
	if tab.(*TableMeta).def == this.def {
		return tab
	} else {
		if v, ok := this.tables[tab.TableName()]; ok {
			return v
		} else {
			return nil
		}
	}
}

//表格的元信息
type TableMeta struct {
	version        int64
	dbVersion      int64
	real_tableName string
	table          string                //表名
	fieldMetas     map[string]*FieldMeta //所有字段元信息
	queryMeta      *QueryMeta
	insertPrefix   string
	selectPrefix   string
	def            *db.DbDef
}

func (t *TableMeta) getRealFieldName(name string) string {
	if f := t.fieldMetas[name]; nil != f {
		return f.getRealName()
	} else {
		return ""
	}
}

func (this *TableMeta) TableName() string {
	return this.table
}

func (this *TableMeta) GetFieldMetas() map[string]*FieldMeta {
	return this.fieldMetas
}

func (this *TableMeta) FillDefaultValues(fields map[string]*proto.Field) {
	if nil != fields {
		for name, v := range this.fieldMetas {
			if _, ok := fields[name]; !ok {
				fields[name] = proto.PackField(name, v.GetDefaultValue())
			}
		}
	}
}

func (this *TableMeta) GetAllFieldsName() []string {
	return this.queryMeta.field_names
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
func (this *TableMeta) GetDefaultValue(name string) interface{} {
	m, ok := this.fieldMetas[name]
	if !ok {
		return nil
	} else {
		return m.defaultValue
	}
}

func (this *TableMeta) CheckFieldMeta(field *proto.Field) error {
	m, ok := this.fieldMetas[field.GetName()]
	if !ok {
		return fmt.Errorf("%s not define in table:%s", field.GetName(), this.table)
	}
	if m.tt != field.GetType() {
		return fmt.Errorf("%s has type:%d different with table:%s define type %d", field.GetName(), field.GetType(), this.table, m.tt)
	}
	return nil
}

func (this *TableMeta) CheckFields(fields ...*proto.Field) error {
	for _, v := range fields {
		if err := this.CheckFieldMeta(v); nil != err {
			return err
		}
	}
	return nil
}

func (this *TableMeta) CheckFieldsName(fields []string) error {
	for _, v := range fields {
		_, ok := this.fieldMetas[v]
		if !ok {
			return fmt.Errorf("fileds %s not define in table:%s", v, this.table)
		}
	}
	return nil
}

func getReceiverType(tt proto.ValueType) reflect.Type {
	if tt == proto.ValueType_int {
		var v int64
		return reflect.TypeOf(v)
	} else if tt == proto.ValueType_float {
		var v float64
		return reflect.TypeOf(v)
	} else if tt == proto.ValueType_string {
		var v string
		return reflect.TypeOf(v)
	} else if tt == proto.ValueType_blob {
		var v []byte
		return reflect.TypeOf(v)
	} else {
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

func getConvetor(tt proto.ValueType) func(interface{}) interface{} {
	if tt == proto.ValueType_int {
		return convert_int64
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

func createTableMetas(def *db.DbDef) (map[string]*TableMeta, error) {
	table_metas := map[string]*TableMeta{}
	if nil != def {
		for _, v := range def.TableDefs {
			t_meta := &TableMeta{
				table:          v.Name,
				version:        v.Version,
				dbVersion:      v.DbVersion,
				real_tableName: v.GetRealName(),
				fieldMetas:     map[string]*FieldMeta{},
				queryMeta: &QueryMeta{
					field_names:      []string{},
					real_field_names: []string{},
					field_type:       []reflect.Type{},
					field_convter:    []func(interface{}) interface{}{},
				},
				def: def,
			}

			//插入三个默认字段
			t_meta.queryMeta.real_field_names = append(t_meta.queryMeta.real_field_names, "__key__")
			t_meta.queryMeta.field_type = append(t_meta.queryMeta.field_type, getReceiverType(proto.ValueType_string))
			t_meta.queryMeta.field_convter = append(t_meta.queryMeta.field_convter, getConvetor(proto.ValueType_string))

			t_meta.queryMeta.real_field_names = append(t_meta.queryMeta.real_field_names, "__version__")
			t_meta.queryMeta.field_type = append(t_meta.queryMeta.field_type, getReceiverType(proto.ValueType_int))
			t_meta.queryMeta.field_convter = append(t_meta.queryMeta.field_convter, getConvetor(proto.ValueType_int))

			t_meta.queryMeta.real_field_names = append(t_meta.queryMeta.real_field_names, "__slot__")
			t_meta.queryMeta.field_type = append(t_meta.queryMeta.field_type, getReceiverType(proto.ValueType_int))
			t_meta.queryMeta.field_convter = append(t_meta.queryMeta.field_convter, getConvetor(proto.ValueType_int))

			//处理其它字段
			for _, vv := range v.Fields {
				//字段名不允许以__开头
				if strings.HasPrefix(vv.Name, "__") {
					return nil, errors.New("has prefix _")
				}

				ftype := db.GetTypeByStr(vv.Type)

				if ftype == proto.ValueType_invaild {
					return nil, errors.New("unsupport data type")
				}

				defaultValue := db.GetDefaultValue(ftype, vv.DefautValue)

				if nil == defaultValue {
					return nil, errors.New("no default value")
				}

				t_meta.fieldMetas[vv.Name] = &FieldMeta{
					name:         vv.Name,
					tt:           ftype,
					defaultValue: defaultValue,
					tabVersion:   vv.TabVersion,
				}

				t_meta.queryMeta.field_names = append(t_meta.queryMeta.field_names, vv.Name)

				t_meta.queryMeta.real_field_names = append(t_meta.queryMeta.real_field_names, vv.GetRealName())

				t_meta.queryMeta.field_type = append(t_meta.queryMeta.field_type, getReceiverType(ftype))

				t_meta.queryMeta.field_convter = append(t_meta.queryMeta.field_convter, getConvetor(ftype))
			}

			table_metas[v.Name] = t_meta

			t_meta.selectPrefix = fmt.Sprintf("SELECT %s FROM %s where __key__ in(", strings.Join(t_meta.queryMeta.real_field_names, ","), t_meta.real_tableName)
			t_meta.insertPrefix = fmt.Sprintf("INSERT INTO %s(%s) VALUES (", t_meta.real_tableName, strings.Join(t_meta.queryMeta.real_field_names, ","))

		}
	}
	return table_metas, nil
}

func CreateDbMeta(def *db.DbDef) (db.DBMeta, error) {
	if tables, err := createTableMetas(def); nil != err {
		return nil, err
	} else {
		return &DBMeta{
			def:    def,
			tables: tables,
		}, nil
	}
}

func CreateDbMetaFromJson(j []byte) (db.DBMeta, error) {
	def, err := db.MakeDbDefFromJsonString(j)
	if nil != err {
		return nil, err
	}
	return CreateDbMeta(def)
}
