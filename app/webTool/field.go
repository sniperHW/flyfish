package webTool

import (
	"fmt"
	"strconv"
	"strings"
)

func processFields(fields string) ([]string, error) {
	s := strings.Split(fields, ",")
	if len(fields) == 0 {
		return nil, fmt.Errorf("fields length is 0")
	}

	_fields := []string{}
	for _, field := range s {
		_field, err := makeField(field)
		if err != nil {
			return nil, err
		}
		_fields = append(_fields, _field)
	}

	return _fields, nil
}

func makeField(field string) (string, error) {
	s := strings.Split(field, ":")
	if len(s) < 3 {
		return "", fmt.Errorf("field(%s) is failed ", field)
	}
	name := s[0]
	tt := s[1]
	def := s[2]

	switch tt {
	case "blob":
		tt = "bytea"
		def = fmt.Sprintf(`DEFAULT '%s'`, def)

	case "string":
		tt = "varchar(65535)"
		def = fmt.Sprintf(`DEFAULT '%s'`, def)

	case "int":
		tt = "int8"
		num, err := strconv.Atoi(def)
		if err != nil {
			return "", fmt.Errorf("field type(int) default value failed: %s", err.Error())
		}
		def = fmt.Sprintf(`DEFAULT %d`, num)

	case "uint":
		tt = "int8"
		num, err := strconv.Atoi(def)
		if err != nil {
			return "", fmt.Errorf("field type(uint) default value failed: %s", err.Error())
		}
		def = fmt.Sprintf(`DEFAULT %d`, num)

	case "float":
		tt = "float8"
		num, err := strconv.ParseFloat(def, 64)
		if err != nil {
			return "", fmt.Errorf("field type(float) default value failed: %s", err.Error())
		}
		def = fmt.Sprintf(`DEFAULT %f`, num)

	default:
		return "", fmt.Errorf("field type(%s) is failed ", tt)
	}

	ret := fmt.Sprintf(`"%s" %s NOT NULL %s`, name, tt, def)
	return ret, nil
}

func checkTT(tt, v string) (interface{}, error) {
	switch tt {
	case "blob":
		return ([]byte)(v), nil
	case "string":
		return v, nil
	case "int":
		tt = "int8"
		num, err := strconv.Atoi(v)
		if err != nil {
			return nil, fmt.Errorf("field type(int) default value failed: %s", err.Error())
		}
		return num, nil
	case "uint":
		tt = "int8"
		num, err := strconv.Atoi(v)
		if err != nil {
			return "", fmt.Errorf("field type(uint) default value failed: %s", err.Error())
		}
		return num, nil
	case "float":
		tt = "float8"
		num, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return nil, fmt.Errorf("field type(float) default value failed: %s", err.Error())
		}
		return num, nil
	default:
		return nil, fmt.Errorf("field type(%s) is failed ", tt)
	}
}

func processData(rowData interface{}) (string, map[string]interface{}, error) {
	row := rowData.([]interface{})
	key := ""
	columns := map[string]interface{}{}
	for j := 0; j < len(row); j++ {
		item := row[j].(map[string]interface{})
		k := item["column"].(string)
		tt := item["tt"].(string)
		v := item["value"].(string)
		iv, err := checkTT(tt, v)
		if err != nil {
			return "", nil, err
		}
		if k == "__key__" {
			key = v
		} else {
			columns[k] = iv
		}
	}
	return key, columns, nil
}
