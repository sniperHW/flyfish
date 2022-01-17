package sql

import (
	"errors"
	"fmt"
	"github.com/jmoiron/sqlx"
	"github.com/sniperHW/flyfish/db"
	"github.com/sniperHW/flyfish/pkg/buffer"
)

var createSqlStrPgSql = `
CREATE TABLE "%s_%d" (
	"__key__" varchar(255) NOT NULL,
    "__version__" int8 NOT NULL DEFAULT 0,
 	"__slot__" int4 NOT NULL DEFAULT 0,
 	%s
 	PRIMARY KEY ("__key__")
);
`

func appendFieldPgSql(buff []byte, field *db.FieldDef) ([]byte, error) {
	offset := len(buff)
	buff = buffer.AppendString(buff, fmt.Sprintf("\"%s_%d\" ", field.Name, field.TabVersion))

	switch field.Type {
	case "int":
		return buffer.AppendString(buff, fmt.Sprintf("int8 NOT NULL DEFAULT %s", field.DefautValue)), nil
	case "float":
		return buffer.AppendString(buff, fmt.Sprintf("float8 NOT NULL DEFAULT %s", field.DefautValue)), nil
	case "string":
		cap := field.StrCap
		if 0 >= cap {
			cap = 65535
		}
		return buffer.AppendString(buff, fmt.Sprintf("varchar(%d) NOT NULL DEFAULT '%s'", cap, field.DefautValue)), nil
	case "blob":
		return buffer.AppendString(buff, fmt.Sprintf("bytea NOT NULL DEFAULT '%s'::bytea", field.DefautValue)), nil
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

func CreateTables(dbc *sqlx.DB, sqlType string, tabDef ...*db.TableDef) error {
	if sqlType == "pgsql" {
		return createTablesPgSql(dbc, tabDef...)
	} else {
		return errors.New("unsupport sql type")
	}
}

func AlterTable(dbc *sqlx.DB, sqlType string, tabDef *db.TableDef) error {
	if sqlType == "pgsql" {
		return alterTablePgSql(dbc, tabDef)
	} else {
		return errors.New("unsupport sql type")
	}
}

func DropTablePgSql(dbc *sqlx.DB, sqlType string, tabDef *db.TableDef) error {
	if sqlType == "pgsql" {
		return dropTablePgSql(dbc, tabDef)
	} else {
		return errors.New("unsupport sql type")
	}
}
