package webTool

import (
	"fmt"
	"strconv"
)

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
