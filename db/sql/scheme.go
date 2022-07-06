package sql

import (
	"errors"
	"fmt"
	"github.com/jmoiron/sqlx"
	"github.com/sniperHW/flyfish/db"
	"github.com/sniperHW/flyfish/pkg/buffer"
	"sort"
	"strconv"
	"strings"
)

const MaxStringLen int = 16384

//合法的字段和表名name_versionNo
func trimVersion(name string) (string, int64, error) {
	v := strings.Split(name, "_")

	if len(v) < 2 {
		return "", -1, errors.New("invaild name")
	}

	i, err := strconv.ParseInt(v[len(v)-1], 10, 64)

	if nil != err {
		return "", -1, errors.New("invaild name")
	}

	last := strings.LastIndex(name, "_")

	return name[:last], i, nil
}

var tableMetaStr = `
SELECT
	TABLE_NAME,
    COLUMN_NAME,
    DATA_TYPE,
    COLUMN_DEFAULT
FROM
    information_schema.COLUMNS
WHERE
    TABLE_NAME like '%s_%%'
ORDER BY
    TABLE_NAME,
    ORDINAL_POSITION;
`

/////pgsql

var createSqlStrPgSql = `
CREATE TABLE "%s_%d" (
	"__key__" varchar(255) NOT NULL,
    "__version__" int8 NOT NULL DEFAULT 0,
 	"__slot__" int4 NOT NULL DEFAULT 0,
 	%s
 	PRIMARY KEY ("__key__")
);
`

/*
var tableMetaStrPgSql = `
SELECT column_name,data_type ,column_default
FROM information_schema.columns
WHERE (table_schema, table_name) = ('public', '%s')
ORDER BY ordinal_position;
`
*/

func appendFieldPgSql(buff []byte, field *db.FieldDef) ([]byte, error) {
	offset := len(buff)
	buff = buffer.AppendString(buff, fmt.Sprintf("\"%s_%d\" ", field.Name, field.TabVersion))

	switch field.Type {
	case "int":
		return buffer.AppendString(buff, fmt.Sprintf("int8 NOT NULL DEFAULT %s", field.GetDefaultValueStr())), nil
	case "float":
		return buffer.AppendString(buff, fmt.Sprintf("float8 NOT NULL DEFAULT %s", field.GetDefaultValueStr())), nil
	case "string":
		return buffer.AppendString(buff, fmt.Sprintf("varchar NOT NULL DEFAULT '%s'", field.GetDefaultValueStr())), nil
	case "blob":
		return buffer.AppendString(buff, "bytea NULL"), nil
	default:
		return buff[:offset], errors.New("invaild type")
	}
}

func createTablesPgSql(dbc *sqlx.DB, tabDef ...*db.TableDef) error {
	var buff []byte
	var err error

	for _, tab := range tabDef {
		var fields []byte
		for _, v := range tab.Fields {
			fields, err = appendFieldPgSql(fields, v)
			if nil != err {
				return err
			}
			fields = buffer.AppendString(fields, ",\n")
		}
		buff = buffer.AppendString(buff, fmt.Sprintf(createSqlStrPgSql, tab.Name, tab.DbVersion, string(fields)))
	}

	_, err = dbc.Exec(string(buff))

	return err
}

func addFieldsPgSql(dbc *sqlx.DB, tabDef *db.TableDef) error {
	var buff []byte
	var err error
	for _, v := range tabDef.Fields {
		buff = buffer.AppendString(buff, fmt.Sprintf("ALTER TABLE %s_%d ADD ", tabDef.Name, tabDef.DbVersion))
		buff, err = appendFieldPgSql(buff, v)
		if nil != err {
			return err
		}
		buff = buffer.AppendString(buff, ";\n")
	}

	str := string(buff)

	_, err = dbc.Exec(str)

	return err
}

//func dropTablePgSql(dbc *sqlx.DB, tabDef *db.TableDef) error {
//	str := fmt.Sprintf("DROP TABLE %s_%d;", tabDef.Name, tabDef.DbVersion)
//	_, err := dbc.Exec(str)
//	return err
//}

type table_scheme struct {
	name            string
	realTabname     string
	dbversion       int64
	column_names    []string
	column_types    []string
	column_defaults []*string
}

func getTableScheme(dbc *sqlx.DB, table string) (*table_scheme, error) {
	rows, err := dbc.Query(fmt.Sprintf(tableMetaStr, table))
	if nil != err {
		return nil, err
	}
	defer rows.Close()

	tables := map[string]*table_scheme{}

	for rows.Next() {
		var table_name string
		var column_name string
		var columm_type string
		var column_default *string

		err = rows.Scan(&table_name, &column_name, &columm_type, &column_default)
		if nil != err {
			return nil, err
		}

		if name, dbversion, err := trimVersion(table_name); nil == err {
			t := tables[table_name]
			if nil == t {
				t = &table_scheme{
					realTabname: table_name,
					name:        name,
					dbversion:   dbversion,
				}
				tables[table_name] = t
			}

			t.column_names = append(t.column_names, column_name)
			t.column_types = append(t.column_types, columm_type)
			t.column_defaults = append(t.column_defaults, column_default)
		}
	}

	if len(tables) == 0 {
		return nil, nil
	} else {
		var tableArray []*table_scheme

		for _, v := range tables {
			tableArray = append(tableArray, v)
		}

		//返回版本号最大的
		sort.Slice(tableArray, func(i, j int) bool {
			return tableArray[i].dbversion > tableArray[j].dbversion
		})

		return tableArray[0], nil
	}
}

func getTableSchemePgSql(dbc *sqlx.DB, table string) (*db.TableDef, error) {

	if t, err := getTableScheme(dbc, table); nil == t {
		return nil, err
	} else {

		tab := &db.TableDef{
			Name:      t.name,
			DbVersion: t.dbversion,
		}

		var keyFieldOk bool
		var versionFieldOk bool
		var slotFieldOk bool

		getDataType := func(tt string) string {
			switch tt {
			case "character varying":
				return "string"
			case "bigint":
				return "int"
			case "bytea":
				return "blob"
			case "double precision":
				return "float"
			default:
				return "unsupported_type"
			}
		}

		getDefaultValue := func(tt string, defaultV *string) *string {
			if tt != "bytea" && nil == defaultV {
				return nil
			}

			switch tt {
			case "character varying":
				v := (*defaultV)[strings.Index(*defaultV, "'")+1 : strings.LastIndex(*defaultV, "'")]
				return &v
			case "bigint":
				return defaultV
			case "bytea":
				defaultV = new(string)
				return defaultV
			case "double precision":
				return defaultV
			default:
				return nil
			}
		}

		for i := 0; i < len(t.column_names); i++ {
			switch t.column_names[i] {
			case "__key__":
				if t.column_types[i] != "character varying" {
					return nil, fmt.Errorf("table:%s __key__ field is not varchar", t.name)
				} else {
					keyFieldOk = true
				}
			case "__version__":
				if t.column_types[i] != "bigint" {
					return nil, fmt.Errorf("table:%s __version__ field is not bigint", t.name)
				} else {
					versionFieldOk = true
				}
			case "__slot__":
				if t.column_types[i] != "integer" {
					return nil, fmt.Errorf("table:%s __slot__ field is not integer", t.name)

				} else {
					slotFieldOk = true
				}
			default:
				real_field_name, version, err := trimVersion(t.column_names[i])
				if nil != err {
					return nil, fmt.Errorf("table:%s %s", t.name, err.Error())
				}

				defaultValue := getDefaultValue(t.column_types[i], t.column_defaults[i])
				if nil == defaultValue {
					return nil, fmt.Errorf("table:%s field:%s missing defaultValue", t.name, real_field_name)
				}

				field := &db.FieldDef{
					TabVersion:   version,
					Name:         real_field_name,
					Type:         getDataType(t.column_types[i]),
					DefaultValue: *defaultValue,
				}

				tab.Fields = append(tab.Fields, field)
			}
		}

		if nil != tab {
			if !keyFieldOk {
				return nil, fmt.Errorf("table:%s missing __key__", t.name)
			}

			if !versionFieldOk {
				return nil, fmt.Errorf("table:%s missing __version__", t.name)
			}

			if !slotFieldOk {
				return nil, fmt.Errorf("table:%s missing __slot__", t.name)
			}
		}

		return tab, nil
	}
}

/////mysql
var createSqlStrMySql = strings.Join(
	[]string{
		"CREATE TABLE %s_%d (",
		"`__key__` VARCHAR(255) NOT NULL,",
		"`__version__` BIGINT NOT NULL DEFAULT '0',",
		"`__slot__` INT NOT NULL DEFAULT '0',",
		"%s PRIMARY KEY ( `__key__` )",
		")",
		"ENGINE=InnoDB",
		"DEFAULT CHARSET=gb2312",
		"COLLATE=gb2312_chinese_ci;",
	}, "\n")

func appendFieldMySql(buff []byte, field *db.FieldDef) ([]byte, error) {
	offset := len(buff)
	buff = buffer.AppendString(buff, fmt.Sprintf("`%s_%d` ", field.Name, field.TabVersion))

	switch field.Type {
	case "int":
		return buffer.AppendString(buff, fmt.Sprintf("BIGINT  NOT NULL DEFAULT '%s'", field.GetDefaultValueStr())), nil
	case "float":
		return buffer.AppendString(buff, fmt.Sprintf("FLOAT NOT NULL DEFAULT '%s'", field.GetDefaultValueStr())), nil
	case "string":
		return buffer.AppendString(buff, fmt.Sprintf("VARCHAR(%d) NOT NULL DEFAULT '%s'", MaxStringLen, field.GetDefaultValueStr())), nil
	case "blob":
		return buffer.AppendString(buff, "BLOB NULL"), nil
	default:
		return buff[:offset], errors.New("invaild type")
	}
}

func createTablesMySql(dbc *sqlx.DB, tabDef ...*db.TableDef) error {
	var buff []byte
	var err error

	for _, tab := range tabDef {
		var fields []byte
		for _, v := range tab.Fields {
			fields, err = appendFieldMySql(fields, v)
			if nil != err {
				return err
			}
			fields = buffer.AppendString(fields, ",\n")
		}
		buff = buffer.AppendString(buff, fmt.Sprintf(createSqlStrMySql, tab.Name, tab.DbVersion, string(fields)))
	}
	_, err = dbc.Exec(string(buff))

	return err
}

//alter table test_0 add column (`field5_0` BIGINT  NOT NULL DEFAULT '1', `field6_0` FLOAT NOT NULL DEFAULT '1.1');
func addFieldsMySql(dbc *sqlx.DB, tabDef *db.TableDef) error {
	var buff []byte
	var err error
	buff = buffer.AppendString(buff, fmt.Sprintf("ALTER TABLE %s_%d ADD column(", tabDef.Name, tabDef.DbVersion))
	for k, v := range tabDef.Fields {
		if k > 0 {
			buff = buffer.AppendString(buff, ",")
		}
		buff, err = appendFieldMySql(buff, v)
		if nil != err {
			return err
		}
	}
	buff = buffer.AppendString(buff, ");\n")
	str := string(buff)
	_, err = dbc.Exec(str)

	return err
}

//func dropTableMySql(dbc *sqlx.DB, tabDef *db.TableDef) error {
//	str := fmt.Sprintf("DROP TABLE %s_%d;", tabDef.Name, tabDef.DbVersion)
//	_, err := dbc.Exec(str)
//	return err
//}

func getTableSchemeMySql(dbc *sqlx.DB, table string) (*db.TableDef, error) {
	if t, err := getTableScheme(dbc, table); nil == t {
		return nil, err
	} else {

		tab := &db.TableDef{
			Name:      t.name,
			DbVersion: t.dbversion,
		}

		var keyFieldOk bool
		var versionFieldOk bool
		var slotFieldOk bool

		getDataType := func(tt string) string {
			switch tt {
			case "varchar":
				return "string"
			case "bigint":
				return "int"
			case "blob":
				return "blob"
			case "float":
				return "float"
			default:
				return "unsupported_type"
			}
		}

		getDefaultValue := func(tt string, defaultV *string) *string {
			if tt == "blob" {
				defaultV = new(string)
			}
			return defaultV
		}

		for i := 0; i < len(t.column_names); i++ {
			switch t.column_names[i] {
			case "__key__":
				if t.column_types[i] != "varchar" {
					return nil, fmt.Errorf("table:%s __key__ field is not varchar", t.name)
				} else {
					keyFieldOk = true
				}
			case "__version__":
				if t.column_types[i] != "bigint" {
					return nil, fmt.Errorf("table:%s __version__ field is not bigint", t.name)
				} else {
					versionFieldOk = true
				}
			case "__slot__":
				if t.column_types[i] != "int" {
					return nil, fmt.Errorf("table:%s __slot__ field is not int", t.name)

				} else {
					slotFieldOk = true
				}
			default:
				real_field_name, version, err := trimVersion(t.column_names[i])
				if nil != err {
					return nil, fmt.Errorf("table:%s %s", t.name, err.Error())
				}

				defaultValue := getDefaultValue(t.column_types[i], t.column_defaults[i])
				if nil == defaultValue {
					return nil, fmt.Errorf("table:%s field:%s missing defaultValue", t.name, real_field_name)
				}

				field := &db.FieldDef{
					TabVersion:   version,
					Name:         real_field_name,
					Type:         getDataType(t.column_types[i]),
					DefaultValue: *defaultValue,
				}

				tab.Fields = append(tab.Fields, field)
			}
		}

		if nil != tab {
			if !keyFieldOk {
				return nil, fmt.Errorf("table:%s missing __key__", t.name)
			}

			if !versionFieldOk {
				return nil, fmt.Errorf("table:%s missing __version__", t.name)
			}

			if !slotFieldOk {
				return nil, fmt.Errorf("table:%s missing __slot__", t.name)
			}
		}

		return tab, nil
	}

}

func CreateTables(dbc *sqlx.DB, sqlType string, tabDef ...*db.TableDef) error {
	if sqlType == "pgsql" {
		return createTablesPgSql(dbc, tabDef...)
	} else {
		return createTablesMySql(dbc, tabDef...)
	}
}

func AddFields(dbc *sqlx.DB, sqlType string, tabDef *db.TableDef) error {
	if sqlType == "pgsql" {
		return addFieldsPgSql(dbc, tabDef)
	} else {
		return addFieldsMySql(dbc, tabDef)
	}
}

func DropTable(dbc *sqlx.DB, tabDef *db.TableDef) error {
	str := fmt.Sprintf("DROP TABLE %s_%d;", tabDef.Name, tabDef.DbVersion)
	_, err := dbc.Exec(str)
	return err
}

func GetTableScheme(dbc *sqlx.DB, sqlType string, name string) (tabdef *db.TableDef, err error) {
	if sqlType == "pgsql" {
		tabdef, err = getTableSchemePgSql(dbc, name)
	} else {
		tabdef, err = getTableSchemeMySql(dbc, name)
	}
	return
}

func IsTableExist(dbc *sqlx.DB, sqlType string, name string, version int64) (bool, error) {
	str := fmt.Sprintf("select count(TABLE_NAME) from information_schema.COLUMNS where TABLE_NAME = '%s_%d'", name, version)

	rows, err := dbc.Query(str)
	if nil != err {
		return false, err
	}
	defer rows.Close()

	var count int

	for rows.Next() {
		err = rows.Scan(&count)
		if nil != err {
			return false, err
		}
	}

	return count > 0, nil
}

func ClearTableData(dbc *sqlx.DB, meta db.DBMeta, name string) error {
	if tmeta := meta.GetTableMeta(name); nil == tmeta {
		return fmt.Errorf("table %s not found", name)
	} else {
		return clearTableData(dbc, tmeta.(*TableMeta).real_tableName)
	}
}

func ClearAllTableData(dbc *sqlx.DB, meta db.DBMeta) error {
	for _, v := range meta.(*DBMeta).tables {
		if err := clearTableData(dbc, v.real_tableName); nil != err {
			return err
		}
	}
	return nil
}

func clearTableData(dbc *sqlx.DB, table_real_name string) error {
	_, err := dbc.Exec(fmt.Sprintf("delete from %s;", table_real_name))
	return err
}

/*
func createBloomFilterMysql(dbc *sqlx.DB) error {
	str := "CREATE TABLE __bloomfilter__ (`slot` INT NOT NULL,`filter` BLOB,PRIMARY KEY (`slot`))\nENGINE=InnoDB\nDEFAULT CHARSET=gb2312\nCOLLATE=gb2312_chinese_ci;"
	_, err := dbc.Exec(str)
	return err
}

func createBloomFilterPgsql(dbc *sqlx.DB) error {
	str := `CREATE TABLE "__bloomfilter__" ("slot" int4 NOT NULL,"filter" bytea,PRIMARY KEY ("slot"));`
	_, err := dbc.Exec(str)
	return err
}

func CreateBloomFilter(sqlType string, dbc *sqlx.DB) error {
	var err error
	if sqlType == "mysql" {
		err = createBloomFilterMysql(dbc)
	} else {
		err = createBloomFilterPgsql(dbc)
	}

	if nil == err {
		return nil
	}

	if strings.Contains(err.Error(), "already exists") {
		return nil
	} else {
		return err
	}
}

func GetBloomFilter(dbc *sqlx.DB, slot int) ([]byte, error) {
	str := fmt.Sprintf(`select filter from __bloomfilter__ where slot=%d;`, slot)
	rows, err := dbc.Query(str)
	if nil != err {
		return nil, err
	}

	defer rows.Close()

	filter := []byte{}
	if rows.Next() {
		err = rows.Scan(&filter)
		return filter, err
	} else {
		return nil, nil
	}
}

func setBloomFilterPgsql(dbc *sqlx.DB, slot int, filter []byte) error {
	str := `INSERT INTO __bloomfilter__(slot, filter) VALUES($1, $2) ON conflict(slot) DO UPDATE SET filter = $2 where __bloomfilter__.slot=$1;`
	_, err := dbc.Exec(str, slot, filter)
	return err
}

func setBloomFilterMysql(dbc *sqlx.DB, slot int, filter []byte) error {
	str := `INSERT INTO __bloomfilter__(slot, filter) VALUES(?, ?) on duplicate key update filter=?;`
	_, err := dbc.Exec(str, slot, filter, filter)
	return err
}

func SetBloomFilter(sqlType string, dbc *sqlx.DB, slot int, filter []byte) error {
	if sqlType == "mysql" {
		return setBloomFilterMysql(dbc, slot, filter)
	} else {
		return setBloomFilterPgsql(dbc, slot, filter)
	}
}

func ClearBloomFilter(dbc *sqlx.DB) error {
	_, err := dbc.Exec("delete from __bloomfilter__;")
	return err
}
*/
