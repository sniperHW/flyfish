package sql

import (
	"encoding/json"
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
	field_names    []string //所有的字段名
	field_convter  []func(interface{}) interface{}
	field_receiver []reflect.Type
}

func (this *QueryMeta) GetFieldNames() []string {
	return this.field_names
}

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

func (this *QueryMeta) GetReceiver() []interface{} {
	receiver := make([]interface{}, len(this.field_receiver))
	for i, v := range this.field_receiver {
		receiver[i] = reflect.New(v).Interface()
	}
	return receiver
}

//字段元信息
type FieldMeta struct {
	name         string          //字段名
	tt           proto.ValueType //字段类型
	defaultValue interface{}     //字段默认值
}

func (this *FieldMeta) GetDefaultValue() interface{} {
	return this.defaultValue
}

type dBMetaJson struct {
	Version int64
	Def     *db.DbDef
}

type DBMeta struct {
	sync.RWMutex
	tables  map[string]*TableMeta
	version int64
	def     *db.DbDef
}

func (this *DBMeta) GetDef() *db.DbDef {
	this.RLock()
	defer this.RUnlock()
	return this.def
}

func (this *DBMeta) ToJson() ([]byte, error) {
	j := dBMetaJson{
		Version: this.version,
		Def:     this.def,
	}
	return json.Marshal(&j)
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
	return this.version
}

func (this *DBMeta) MoveTo(other db.DBMeta) {
	this.Lock()
	defer this.Unlock()
	o := other.(*DBMeta)
	o.Lock()
	defer o.Unlock()
	if nil != this.tables {
		o.def = this.def
		o.version = this.version
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
	table            string                //表名
	fieldMetas       map[string]*FieldMeta //所有字段元信息
	queryMeta        *QueryMeta
	insertPrefix     string
	selectPrefix     string
	insertFieldOrder []string
	def              *db.DbDef
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
	return this.insertFieldOrder
}

func (this *TableMeta) GetInsertOrder() []string {
	return this.insertFieldOrder
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
			fmt.Errorf("fileds %s not define in table:%s", v, this.table)
		}
	}
	return nil
}

func getReceiver(tt proto.ValueType) reflect.Type {
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
				table:            v.Name,
				fieldMetas:       map[string]*FieldMeta{},
				insertFieldOrder: []string{},
				queryMeta: &QueryMeta{
					field_names:    []string{},
					field_receiver: []reflect.Type{},
					field_convter:  []func(interface{}) interface{}{},
				},
				def: def,
			}

			//插入三个默认字段
			t_meta.queryMeta.field_names = append(t_meta.queryMeta.field_names, "__key__")
			t_meta.queryMeta.field_receiver = append(t_meta.queryMeta.field_receiver, getReceiver(proto.ValueType_string))
			t_meta.queryMeta.field_convter = append(t_meta.queryMeta.field_convter, getConvetor(proto.ValueType_string))

			t_meta.queryMeta.field_names = append(t_meta.queryMeta.field_names, "__version__")
			t_meta.queryMeta.field_receiver = append(t_meta.queryMeta.field_receiver, getReceiver(proto.ValueType_int))
			t_meta.queryMeta.field_convter = append(t_meta.queryMeta.field_convter, getConvetor(proto.ValueType_int))

			t_meta.queryMeta.field_names = append(t_meta.queryMeta.field_names, "__slot__")
			t_meta.queryMeta.field_receiver = append(t_meta.queryMeta.field_receiver, getReceiver(proto.ValueType_int))
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
				}

				t_meta.insertFieldOrder = append(t_meta.insertFieldOrder, vv.Name)

				t_meta.queryMeta.field_names = append(t_meta.queryMeta.field_names, vv.Name)

				t_meta.queryMeta.field_receiver = append(t_meta.queryMeta.field_receiver, getReceiver(ftype))

				t_meta.queryMeta.field_convter = append(t_meta.queryMeta.field_convter, getConvetor(ftype))
			}

			table_metas[v.Name] = t_meta

			t_meta.selectPrefix = fmt.Sprintf("SELECT %s FROM %s where __key__ in(", strings.Join(t_meta.queryMeta.field_names, ","), t_meta.table)
			t_meta.insertPrefix = fmt.Sprintf("INSERT INTO %s(__key__,__version__,__slot__,%s) VALUES (", t_meta.table, strings.Join(t_meta.insertFieldOrder, ","))

		}
	}
	return table_metas, nil
}

func CreateDbMeta(version int64, def *db.DbDef) (db.DBMeta, error) {
	if tables, err := createTableMetas(def); nil != err {
		return nil, err
	} else {
		return &DBMeta{
			version: version,
			def:     def,
			tables:  tables,
		}, nil
	}
}

func CreateDbMetaFromJson(j []byte) (db.DBMeta, error) {
	var jj dBMetaJson
	if err := json.Unmarshal(j, &jj); nil != err {
		return nil, err
	}

	if tables, err := createTableMetas(jj.Def); nil != err {
		return nil, err
	} else {
		return &DBMeta{
			version: jj.Version,
			def:     jj.Def,
			tables:  tables,
		}, nil
	}
}
