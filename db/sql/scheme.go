package sql

import (
	"errors"
	"fmt"
	"github.com/jmoiron/sqlx"
	"github.com/sniperHW/flyfish/db"
	"github.com/sniperHW/flyfish/pkg/buffer"
	"strconv"
	"strings"
)

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
    COLUMN_NAME,
    DATA_TYPE,
    COLUMN_DEFAULT,
    CHARACTER_MAXIMUM_LENGTH
FROM
    information_schema.COLUMNS
WHERE
    TABLE_NAME = '%s'
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
SELECT column_name,data_type ,column_default,character_maximum_length
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
		return buffer.AppendString(buff, fmt.Sprintf("int8 NOT NULL DEFAULT %s", field.DefautValue)), nil
	case "float":
		return buffer.AppendString(buff, fmt.Sprintf("float8 NOT NULL DEFAULT %s", field.DefautValue)), nil
	case "string":
		return buffer.AppendString(buff, fmt.Sprintf("varchar(%d) NOT NULL DEFAULT '%s'", field.StrCap, field.DefautValue)), nil
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

func alterTablePgSql(dbc *sqlx.DB, tabDef *db.TableDef) error {
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

func dropTablePgSql(dbc *sqlx.DB, tabDef *db.TableDef) error {
	str := fmt.Sprintf("DROP TABLE %s_%d;", tabDef.Name, tabDef.DbVersion)
	_, err := dbc.Exec(str)
	return err
}

func getTableSchemePgSql(dbc *sqlx.DB, table string) (*db.TableDef, error) {
	realname, version, err := trimVersion(table)

	if nil != err {
		return nil, err
	}

	var tab *db.TableDef
	rows, err := dbc.Query(fmt.Sprintf(tableMetaStr, table))
	if nil != err {
		return nil, err
	}

	var column_name string
	var data_type string
	var column_default *string
	var character_maximum_length *int

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
			return "invaild"
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

	for rows.Next() {
		if nil == tab {
			tab = &db.TableDef{
				Name:      realname,
				DbVersion: version,
			}
		}

		err = rows.Scan(&column_name, &data_type, &column_default, &character_maximum_length)
		if nil != err {
			return nil, err
		}

		if column_name == "__key__" {
			if data_type != "character varying" {
				fmt.Println("data_type", data_type)
				return nil, errors.New("__key__ field is not varchar")
			} else {
				keyFieldOk = true
			}
		} else if column_name == "__version__" {
			if data_type != "bigint" {
				return nil, errors.New("__version__ field is not bigint")
			} else {
				versionFieldOk = true
			}
		} else if column_name == "__slot__" {
			if data_type != "integer" {
				return nil, errors.New("__slot__ field is not integer")
			} else {
				slotFieldOk = true
			}
		} else {
			real_field_name, version, err := trimVersion(column_name)
			if nil != err {
				return nil, err
			}

			defaultValue := getDefaultValue(data_type, column_default)
			if nil == defaultValue {
				return nil, errors.New(fmt.Sprintf("%s missing defaultValue", real_field_name))
			}

			field := &db.FieldDef{
				TabVersion:  version,
				Name:        real_field_name,
				Type:        getDataType(data_type),
				DefautValue: *defaultValue,
			}

			if nil != character_maximum_length {
				field.StrCap = *character_maximum_length
			}

			tab.Fields = append(tab.Fields, field)
		}
	}

	if nil != tab {
		if !keyFieldOk {
			return nil, errors.New("missing __key__")
		}

		if !versionFieldOk {
			return nil, errors.New("missing __version__")
		}

		if !slotFieldOk {
			return nil, errors.New("missing __slot__")
		}
	}

	return tab, nil
}

/////mysql
var createSqlStrMySql = strings.Join(
	[]string{
		"CREATE TABLE %s_%d (",
		"`__key__` varchar(255) NOT NULL,",
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
		return buffer.AppendString(buff, fmt.Sprintf("BIGINT  NOT NULL DEFAULT '%s'", field.DefautValue)), nil
	case "float":
		return buffer.AppendString(buff, fmt.Sprintf("FLOAT NOT NULL DEFAULT '%s'", field.DefautValue)), nil
	case "string":
		cap := field.StrCap
		if 0 >= cap {
			cap = 65535
		}
		return buffer.AppendString(buff, fmt.Sprintf("varchar(%d) NOT NULL DEFAULT '%s'", cap, field.DefautValue)), nil
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
func alterTableMySql(dbc *sqlx.DB, tabDef *db.TableDef) error {
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

func dropTableMySql(dbc *sqlx.DB, tabDef *db.TableDef) error {
	str := fmt.Sprintf("DROP TABLE %s_%d;", tabDef.Name, tabDef.DbVersion)
	_, err := dbc.Exec(str)
	return err
}

func getTableSchemeMySql(dbc *sqlx.DB, table string) (*db.TableDef, error) {
	realname, version, err := trimVersion(table)

	if nil != err {
		return nil, err
	}

	var tab *db.TableDef
	rows, err := dbc.Query(fmt.Sprintf(tableMetaStr, table))
	if nil != err {
		return nil, err
	}

	var column_name string
	var data_type string
	var column_default *string
	var character_maximum_length *int

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
			return "invaild"
		}
	}

	getDefaultValue := func(tt string, defaultV *string) *string {
		if tt == "blob" {
			defaultV = new(string)
		}
		return defaultV
	}

	for rows.Next() {
		if nil == tab {
			tab = &db.TableDef{
				Name:      realname,
				DbVersion: version,
			}
		}

		err = rows.Scan(&column_name, &data_type, &column_default, &character_maximum_length)
		if nil != err {
			return nil, err
		}

		if column_name == "__key__" {
			if data_type != "varchar" {
				return nil, errors.New("__key__ field is not varchar")
			} else {
				keyFieldOk = true
			}
		} else if column_name == "__version__" {
			if data_type != "bigint" {
				return nil, errors.New("__version__ field is not bigint")
			} else {
				versionFieldOk = true
			}
		} else if column_name == "__slot__" {
			if data_type != "int" {
				return nil, errors.New("__slot__ field is not int")
			} else {
				slotFieldOk = true
			}
		} else {
			real_field_name, version, err := trimVersion(column_name)
			if nil != err {
				return nil, err
			}

			defaultValue := getDefaultValue(data_type, column_default)
			if nil == defaultValue {
				return nil, errors.New(fmt.Sprintf("%s missing defaultValue", real_field_name))
			}

			field := &db.FieldDef{
				TabVersion:  version,
				Name:        real_field_name,
				Type:        getDataType(data_type),
				DefautValue: *defaultValue,
			}

			if nil != character_maximum_length {
				field.StrCap = *character_maximum_length
			}

			tab.Fields = append(tab.Fields, field)
		}
	}

	if nil != tab {
		if !keyFieldOk {
			return nil, errors.New("missing __key__")
		}

		if !versionFieldOk {
			return nil, errors.New("missing __version__")
		}

		if !slotFieldOk {
			return nil, errors.New("missing __slot__")
		}
	}

	return tab, nil
}

func CreateTables(dbc *sqlx.DB, sqlType string, tabDef ...*db.TableDef) error {
	if sqlType == "pgsql" {
		return createTablesPgSql(dbc, tabDef...)
	} else {
		return createTablesMySql(dbc, tabDef...)
	}
}

func AlterTable(dbc *sqlx.DB, sqlType string, tabDef *db.TableDef) error {
	if sqlType == "pgsql" {
		return alterTablePgSql(dbc, tabDef)
	} else {
		return alterTableMySql(dbc, tabDef)
	}
}

func DropTable(dbc *sqlx.DB, sqlType string, tabDef *db.TableDef) error {
	if sqlType == "pgsql" {
		return dropTablePgSql(dbc, tabDef)
	} else {
		return dropTableMySql(dbc, tabDef)
	}
}

func GetTableScheme(dbc *sqlx.DB, sqlType string, table_real_name string) (*db.TableDef, error) {
	if sqlType == "pgsql" {
		return getTableSchemePgSql(dbc, table_real_name)
	} else {
		return getTableSchemeMySql(dbc, table_real_name)
	}
}
