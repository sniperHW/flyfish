package db

import (
	"fmt"
	"strconv"
	"strings"
)

/*
 * 生成表sql语句
 */
func (this *Client) DumpSql(IsGetData bool) (map[string]string, error) {
	ret, err := this.GetAll("table_conf", []string{"__table__", "__conf__"})
	if err != nil {
		return nil, err
	}

	table_conf := `
DROP TABLE IF EXISTS "table_conf";
CREATE TABLE "table_conf" (
"__table__" varchar(255) NOT NULL DEFAULT '',
"__conf__" varchar(65535) NOT NULL DEFAULT '',
PRIMARY KEY ("__table__")
);`
	insert := makeInsertSql("table_conf", ret)

	result := map[string]string{}
	for _, item := range ret {
		name := item["__table__"].(string)
		conf := item["__conf__"].(string)
		fields, _ := ProcessFields(this.tt, conf)

		v := makeCreateSql(name, fields)
		if IsGetData {
			tData, err := this.getAll(name)
			if err != nil {
				return nil, err
			}
			v += makeInsertSql(name, tData)
		}
		result[name] = v
	}

	result["table_conf"] = table_conf + insert

	return result, nil

}

func (this *Client) getAll(tableName string) (ret []map[string]interface{}, err error) {
	sqlStatement := fmt.Sprintf(`SELECT * FROM "%s";`, tableName)
	rows, err := this.dbConn.Query(sqlStatement)
	if err != nil {
		return nil, err
	}

	ret = []map[string]interface{}{}
	for rows.Next() {
		cols, _ := rows.Columns()
		values := []interface{}{}

		for i := 0; i < len(cols); i++ {
			values = append(values, new(interface{}))
		}
		err := rows.Scan(values...)
		if err != nil {
			return nil, err
		}

		mid := map[string]interface{}{}
		for i, k := range cols {
			mid[k] = *(values[i].(*interface{}))
		}
		ret = append(ret, mid)
	}

	return ret, nil
}

func makeCreateSql(tableName string, fields []string) string {
	sqlStr := `
DROP TABLE IF EXISTS "%s";
CREATE TABLE "%s" (
  "__key__" varchar(255) NOT NULL,
  "__version__" int8 NOT NULL,
  %s
  PRIMARY KEY ("__key__")
);`

	columns := ""
	for _, field := range fields {
		columns += field + ","
	}

	sqlStatement := fmt.Sprintf(sqlStr, tableName, tableName, columns)
	return sqlStatement
}

func makeInsertSql(tableName string, data []map[string]interface{}) string {
	sqlStr := `
INSERT INTO "%s" (%s) VALUES (%s);`

	ret := ""
	for _, fields := range data {
		keys, values := []string{}, []string{}
		for k, v := range fields {
			keys = append(keys, k)
			switch v.(type) {
			case string:
				values = append(values, fmt.Sprintf(`'%s'`, v))
			default:
				values = append(values, fmt.Sprintf(`%v`, v))
			}

		}
		sqlStatement := fmt.Sprintf(sqlStr, tableName, strings.Join(keys, ","), strings.Join(values, ","))
		ret += sqlStatement
	}

	return ret
}

func ProcessFields(tt, fields string) ([]string, error) {
	s := strings.Split(fields, ",")
	if len(fields) == 0 {
		return nil, fmt.Errorf("fields length is 0")
	}

	_fields := []string{}
	for _, field := range s {
		_field, err := makeField(tt, field)
		if err != nil {
			return nil, err
		}
		_fields = append(_fields, _field)
	}

	return _fields, nil
}

func makeField(tt, field string) (string, error) {
	s := strings.Split(field, ":")
	if len(s) < 3 {
		return "", fmt.Errorf("field(%s) is failed ", field)
	}
	name := s[0]
	t := s[1]
	def := s[2]

	switch t {
	case "blob":
		if tt == "postgres" {
			t = "bytea"
			def = fmt.Sprintf(`DEFAULT '%s'`, def)
		} else if tt == "mysql" {
			t = "blob"
			def = fmt.Sprintf(`DEFAULT '%s'`, def)
		}

	case "string":
		t = "varchar(65535)"
		def = fmt.Sprintf(`DEFAULT '%s'`, def)

	case "int":
		t = "int8"
		num, err := strconv.Atoi(def)
		if err != nil {
			return "", fmt.Errorf("field type(int) default value failed: %s", err.Error())
		}
		def = fmt.Sprintf(`DEFAULT %d`, num)

	case "uint":
		t = "int8"
		num, err := strconv.Atoi(def)
		if err != nil {
			return "", fmt.Errorf("field type(uint) default value failed: %s", err.Error())
		}
		def = fmt.Sprintf(`DEFAULT %d`, num)

	case "float":
		t = "float8"
		num, err := strconv.ParseFloat(def, 64)
		if err != nil {
			return "", fmt.Errorf("field type(float) default value failed: %s", err.Error())
		}
		def = fmt.Sprintf(`DEFAULT %f`, num)

	default:
		return "", fmt.Errorf("field type(%s) is failed ", t)
	}

	ret := fmt.Sprintf(`"%s" %s NOT NULL %s`, name, t, def)
	return ret, nil
}
