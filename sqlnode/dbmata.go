package sqlnode

import (
	"fmt"
	"github.com/jmoiron/sqlx"
	"github.com/sniperHW/flyfish/errcode"
	"github.com/sniperHW/flyfish/net"
	"github.com/sniperHW/flyfish/proto"
	"strconv"
	"strings"
	"sync/atomic"
)

type fieldConverter func(interface{}) interface{}

var (
	fieldTypeName2Type = map[string]proto.ValueType{
		"int":    proto.ValueType_int,
		"float":  proto.ValueType_float,
		"string": proto.ValueType_string,
		"blob":   proto.ValueType_blob,
	}

	fieldType2Getter = map[proto.ValueType]func() interface{}{
		proto.ValueType_int:    func() interface{} { return new(int64) },
		proto.ValueType_float:  func() interface{} { return new(float64) },
		proto.ValueType_string: func() interface{} { return new(string) },
		proto.ValueType_blob:   func() interface{} { return new([]byte) },
	}

	fieldType2Converter = map[proto.ValueType]fieldConverter{
		proto.ValueType_int:    func(v interface{}) interface{} { return *v.(*int64) },
		proto.ValueType_float:  func(v interface{}) interface{} { return *v.(*float64) },
		proto.ValueType_string: func(v interface{}) interface{} { return *v.(*string) },
		proto.ValueType_blob:   func(v interface{}) interface{} { return *v.(*[]byte) },
	}

	fieldType2GetDefaultV = map[proto.ValueType]func(string) interface{}{
		proto.ValueType_int: func(s string) interface{} {
			if s == "" {
				return int64(0)
			} else if v, err := strconv.ParseInt(s, 10, 64); err == nil {
				return v
			} else {
				return nil
			}
		},

		proto.ValueType_float: func(s string) interface{} {
			if s == "" {
				return float64(0)
			} else if v, err := strconv.ParseInt(s, 10, 64); err == nil {
				return v
			} else {
				return nil
			}
		},

		proto.ValueType_string: func(s string) interface{} {
			return s
		},

		proto.ValueType_blob: func(s string) interface{} {
			return []byte{}
		},
	}
)

func getFieldTypeByStr(str string) proto.ValueType {
	if t, ok := fieldTypeName2Type[str]; ok {
		return t
	} else {
		return proto.ValueType_invaild
	}
}

func getFieldGetterByType(t proto.ValueType) func() interface{} {
	return fieldType2Getter[t]
}

func getFieldConverterByType(t proto.ValueType) func(interface{}) interface{} {
	if f, ok := fieldType2Converter[t]; ok {
		return f
	} else {
		return nil
	}
}

func getFieldDefaultValue(t proto.ValueType, str string) interface{} {
	if f, ok := fieldType2GetDefaultV[t]; ok {
		return f(str)
	} else {
		return nil
	}
}

func isValidFieldType(t proto.ValueType) bool {
	return t != proto.ValueType_nil && t != proto.ValueType_invaild
}

type fieldMeta struct {
	name     string          // 字段名
	typ      proto.ValueType // 字段值类型
	defaultV interface{}     // 字段默认值
}

func (f *fieldMeta) getType() proto.ValueType {
	return f.typ
}

func (f *fieldMeta) getDefaultV() interface{} {
	return f.defaultV
}

func (f *fieldMeta) getReceiver() interface{} {
	return getFieldGetterByType(f.typ)()
}

func (f *fieldMeta) getConverter() func(interface{}) interface{} {
	return getFieldConverterByType(f.typ)
}

const (
	keyFieldName      = "__key__"
	keyFieldIndex     = 0
	versionFieldName  = "__version__"
	versionFieldIndex = keyFieldIndex + 1
	FirstFieldIndex   = versionFieldIndex + 1
)

var (
	keyFieldMeta = &fieldMeta{
		name:     keyFieldName,
		typ:      proto.ValueType_string,
		defaultV: getFieldDefaultValue(proto.ValueType_string, ""),
	}

	versionFieldMeta = &fieldMeta{
		name:     versionFieldName,
		typ:      proto.ValueType_int,
		defaultV: getFieldDefaultValue(proto.ValueType_int, ""),
	}
)

type tableMeta struct {
	name               string                // 表名
	fieldMetas         map[string]*fieldMeta // 字段meta
	fieldInsertOrder   []string              // 字段插入排列
	selectAllPrefix    string                // 查找记录所有字段前缀 "select allFieldNames... from table_name where keyFieldName="
	insertPrefix       string                // 记录掺入前缀 "insert into table_name(cols...) VALUES("
	allFieldNames      []string              // 包括'__key__'和'__version__'字段
	allFieldGetters    []func() interface{}  //
	allFieldConverters []fieldConverter      //
}

func (t *tableMeta) getName() string {
	return t.name
}

func (t *tableMeta) getFieldMeta(field string) *fieldMeta {
	return t.fieldMetas[field]
}

func (t *tableMeta) getFieldMetas() map[string]*fieldMeta {
	return t.fieldMetas
}

func (t *tableMeta) getFieldInsertOrder() []string {
	return t.fieldInsertOrder
}

func (t *tableMeta) getInsertPrefix() string {
	return t.insertPrefix
}

func (t *tableMeta) getSelectAllPrefix() string {
	return t.selectAllPrefix
}

func (t *tableMeta) getFieldCount() int {
	return len(t.fieldMetas)
}

func (t *tableMeta) checkFieldNames(fields []string) (bool, int) {
	for i, v := range fields {
		if t.fieldMetas[v] == nil {
			return false, i
		}
	}

	return true, 0
}

func (t *tableMeta) checkFields(fields []*proto.Field) (bool, int) {
	for i, v := range fields {
		fm := t.fieldMetas[v.GetName()]
		if fm == nil || fm.getType() != v.GetType() {
			return false, i
		}
	}

	return true, 0
}

func (t *tableMeta) checkField(field *proto.Field) bool {
	fm := t.fieldMetas[field.GetName()]
	return fm != nil && fm.getType() == field.GetType()
}

func (t *tableMeta) getAllFields() []string {
	return t.allFieldNames
}

func (t *tableMeta) getAllFieldReceivers() []interface{} {
	//if len(t.allFieldNewer) != len(t.allFieldNames) {
	//	panic("impossible")
	//}

	receivers := make([]interface{}, len(t.allFieldNames))
	for i, f := range t.allFieldGetters {
		receivers[i] = f()
	}

	return receivers
}

func (t *tableMeta) getAllFieldConverter() []fieldConverter {
	return t.allFieldConverters
}

type dbMeta struct {
	tableMetas  atomic.Value
	keyMeta     *fieldMeta
	versionMeta *fieldMeta
	version     int64
}

func (d *dbMeta) setTableMetas(tableMetas map[string]*tableMeta) {
	d.tableMetas.Store(tableMetas)
}

func (d *dbMeta) getTableMeta(table string) *tableMeta {
	tableMetas := d.tableMetas.Load().(map[string]*tableMeta)
	return tableMetas[table]
}

type tableDef struct {
	name     string
	fieldDef string
}

var (
	globalDBMeta *dbMeta
)

func initDBMeta() {
	tableDef, err := loadTableDef()
	if err != nil {
		getLogger().Fatalf("init db-meta: load table def: %s.", err)
	}

	tableMetas, err := createTableMetasByTableDef(tableDef)
	if err != nil {
		getLogger().Fatalf("init db-meta: create table meta: %s.", err)
	}

	globalDBMeta = &dbMeta{
		version: 0,
	}
	globalDBMeta.tableMetas.Store(tableMetas)

	getLogger().Infoln("init de-meta.")
}

func createTableMetasByTableDef(def []*tableDef) (map[string]*tableMeta, error) {
	tableMetas := make(map[string]*tableMeta, len(def))
	for _, t := range def {
		fieldDefStr := strings.Split(t.fieldDef, ",")

		fieldCount := len(fieldDefStr)
		if fieldCount == 0 {
			return nil, fmt.Errorf("%s: no field", t.name)
		}

		allFieldCount := fieldCount + 2
		tMeta := &tableMeta{
			name:               t.name,
			fieldMetas:         make(map[string]*fieldMeta, fieldCount),
			fieldInsertOrder:   make([]string, fieldCount),
			allFieldNames:      make([]string, allFieldCount),
			allFieldGetters:    make([]func() interface{}, allFieldCount),
			allFieldConverters: make([]fieldConverter, allFieldCount),
		}

		tMeta.allFieldNames[keyFieldIndex] = keyFieldMeta.name
		tMeta.allFieldGetters[keyFieldIndex] = getFieldGetterByType(keyFieldMeta.typ)
		tMeta.allFieldConverters[keyFieldIndex] = getFieldConverterByType(keyFieldMeta.typ)

		tMeta.allFieldNames[versionFieldIndex] = versionFieldMeta.name
		tMeta.allFieldGetters[versionFieldIndex] = getFieldGetterByType(versionFieldMeta.typ)
		tMeta.allFieldConverters[versionFieldIndex] = getFieldConverterByType(versionFieldMeta.typ)

		var (
			allFieldIndex = FirstFieldIndex
			fieldName     string
			fieldType     proto.ValueType
			fieldDefaultV interface{}
		)

		for i, v := range fieldDefStr {
			s := strings.Split(v, ":")

			if len(s) != 3 {
				return nil, fmt.Errorf("%s: field %dth '%s': invalid", t.name, i, v)
			}

			fieldName = s[0]

			if fieldName == "" || fieldName == keyFieldName || fieldName == versionFieldName {
				return nil, fmt.Errorf("%s: field %dth '%s': name invalid", t.name, i, v)
			}

			if _, ok := tMeta.fieldMetas[s[0]]; ok {
				return nil, fmt.Errorf("%s: field %dth '%s': name repeated", t.name, i, v)
			}

			if fieldType = getFieldTypeByStr(s[1]); !isValidFieldType(fieldType) {
				return nil, fmt.Errorf("%s: field %dth '%s': type invalid", t.name, i, v)
			}

			if fieldDefaultV = getFieldDefaultValue(fieldType, s[2]); fieldDefaultV == nil {
				return nil, fmt.Errorf("%s: field %dth '%s': default-value invalid", t.name, i, v)
			}

			tMeta.fieldMetas[fieldName] = &fieldMeta{
				name:     fieldName,
				typ:      fieldType,
				defaultV: fieldDefaultV,
			}

			tMeta.fieldInsertOrder[i] = fieldName
			tMeta.allFieldNames[allFieldIndex] = fieldName
			tMeta.allFieldGetters[allFieldIndex] = getFieldGetterByType(fieldType)
			tMeta.allFieldConverters[allFieldIndex] = getFieldConverterByType(fieldType)
			allFieldIndex++
		}

		tMeta.insertPrefix = fmt.Sprintf("INSERT INTO %s(%s,%s,%s) VALUES(", t.name, keyFieldName, versionFieldName, strings.Join(tMeta.fieldInsertOrder, ","))
		tMeta.selectAllPrefix = fmt.Sprintf("SELECT %s FROM %s WHERE ", strings.Join(tMeta.allFieldNames, ","), t.name)
		tableMetas[tMeta.name] = tMeta
	}

	return tableMetas, nil
}

func loadTableDef() ([]*tableDef, error) {
	var db *sqlx.DB
	var err error

	db, err = dbOpenByConfig()

	if nil != err {
		return nil, err
	} else {

		if rows, err := db.Queryx("select __table__,__conf__ from table_conf;"); err != nil {
			return nil, err
		} else {
			var defs []*tableDef

			for rows.Next() {
				def := new(tableDef)
				if err = rows.Scan(&def.name, &def.fieldDef); err != nil {
					break
				}

				defs = append(defs, def)
			}

			_ = rows.Close()

			_ = db.Close()

			return defs, err
		}
	}
}

func onReloadTableConf(cli *cliConn, msg *net.Message) {
	head := msg.GetHead()

	var (
		tableDef   []*tableDef
		tableMetas map[string]*tableMeta
		err        error
		resp       = &proto.ReloadTableConfResp{}
	)

	if tableDef, err = loadTableDef(); err != nil {
		resp.Err = fmt.Sprintf("load table-def: %s", err)
		getLogger().Errorf(resp.Err)
		head.ErrCode = errcode.ERR_OTHER
	} else if tableMetas, err = createTableMetasByTableDef(tableDef); err != nil {
		resp.Err = fmt.Sprintf("create table-meta: %s", err)
		getLogger().Errorln(resp.Err)
		head.ErrCode = errcode.ERR_OTHER
	} else {
		globalDBMeta.setTableMetas(tableMetas)
		head.ErrCode = errcode.ERR_OK
	}

	_ = cli.sendMessage(net.NewMessage(head, resp))
}

func getDBMeta() *dbMeta {
	return globalDBMeta
}
