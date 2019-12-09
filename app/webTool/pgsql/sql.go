package pgsql

import (
	"fmt"
	"strings"
)

/*
 * 插入数据
 * tableName:表名 fields:键值对
 */
func (this *Client) Set(tableName string, fields map[string]interface{}) error {
	sqlStr := `
INSERT INTO "%s" (%s)
VALUES (%s);`

	keys, values := []string{}, []string{}
	args := []interface{}{}
	var i = 1
	for k, v := range fields {
		keys = append(keys, k)
		values = append(values, fmt.Sprintf("$%d", i))
		i++
		args = append(args, v)
	}

	sqlStatement := fmt.Sprintf(sqlStr, tableName, strings.Join(keys, ","), strings.Join(values, ","))
	smt, err := this.dbConn.Prepare(sqlStatement)
	if err != nil {
		return err
	}
	_, err = smt.Exec(args...)
	return err
}

/*
 * 更新数据
 * tableName:表名 whereStr:选择规则 fields:键值对
 */
func (this *Client) Update(tableName, whereStr string, fields map[string]interface{}) error {
	sqlStr := `
UPDATE "%s" 
SET %s
WHERE %s;`

	keys := []string{}
	args := []interface{}{}
	var i = 1
	for k, v := range fields {
		keys = append(keys, fmt.Sprintf(`%s = $%d`, k, i))
		i++
		args = append(args, v)
	}

	sqlStatement := fmt.Sprintf(sqlStr, tableName, strings.Join(keys, ","), whereStr)
	//fmt.Println(sqlStatement)
	smt, err := this.dbConn.Prepare(sqlStatement)
	if err != nil {
		return err
	}
	_, err = smt.Exec(args...)
	return err

}

/*
 * 没有数据插入，有则添加。
 * tableName:表名 key:主键名 fields:键值对，包含主键
 */
func (this *Client) SetNx(tableName, key string, fields map[string]interface{}) error {
	sqlStr := `
INSERT INTO "%s" (%s)
VALUES(%s) 
ON conflict(%s) DO 
UPDATE SET %s;`

	keys, values, sets := []string{}, []string{}, []string{}
	args := []interface{}{}
	var i = 1
	for k, v := range fields {
		keys = append(keys, k)
		values = append(values, fmt.Sprintf("$%d", i))
		if key != k {
			sets = append(sets, fmt.Sprintf(`%s = $%d`, k, i))
		}
		i++
		args = append(args, v)
	}

	sqlStatement := fmt.Sprintf(sqlStr, tableName, strings.Join(keys, ","), strings.Join(values, ","), key, strings.Join(sets, ","))
	//fmt.Println(sqlStatement)
	smt, err := this.dbConn.Prepare(sqlStatement)
	if err != nil {
		return err
	}
	_, err = smt.Exec(args...)
	return err
}

/*
 * 读取数据。
 * tableName:表名 whereStr:选择规则 fields:要查询的键名
 * ret 返回键值对
 */
func (this *Client) Get(tableName, whereStr string, fields []string) (ret map[string]interface{}, err error) {
	sqlStr := `
SELECT %s FROM "%s" 
WHERE %s;`

	keys := []string{}
	values := []interface{}{}
	for _, k := range fields {
		keys = append(keys, k)
		values = append(values, new(interface{}))
	}

	sqlStatement := fmt.Sprintf(sqlStr, strings.Join(keys, ","), tableName, whereStr)
	//fmt.Println(sqlStatement)
	row := this.dbConn.QueryRow(sqlStatement)
	err = row.Scan(values...)
	if err != nil {
		return nil, err
	}

	ret = map[string]interface{}{}
	for i, k := range fields {
		ret[k] = *(values[i].(*interface{}))
	}

	return ret, nil
}

/*
 * 读取所有数据。
 * tableName:表名 fields:要查询的键名
 * ret 返回键值对的slice
 */
func (this *Client) GetAll(tableName string, fields []string) (ret []map[string]interface{}, err error) {
	keys := []string{}
	values := []interface{}{}
	for _, k := range fields {
		keys = append(keys, k)
		values = append(values, new(interface{}))
	}

	sqlStatement := fmt.Sprintf(`SELECT %s FROM "%s";`, strings.Join(keys, ","), tableName)
	rows, err := this.dbConn.Query(sqlStatement)
	if err != nil {
		return nil, err
	}

	ret = []map[string]interface{}{}
	for rows.Next() {
		err := rows.Scan(values...)
		if err != nil {
			return nil, err
		}

		mid := map[string]interface{}{}
		for i, k := range fields {
			mid[k] = *(values[i].(*interface{}))
		}
		ret = append(ret, mid)
	}

	return ret, nil
}

func (this *Client) Create(tableName string, fields []string) error {
	sqlStr := `CREATE TABLE "public"."%s" (
  "__key__" varchar(255) NOT NULL,
  "__version__" int8 NOT NULL,
  %s
  PRIMARY KEY ("__key__")
);`
	columns := ""
	for _, field := range fields {
		columns += field + ","
	}

	sqlStatement := fmt.Sprintf(sqlStr, tableName, columns)
	//fmt.Println(sqlStatement)
	smt, err := this.dbConn.Prepare(sqlStatement)
	if err != nil {
		return err
	}
	_, err = smt.Exec()
	return err
}

// ADD、DROP
// field = "test" int8 NOT NULL DEFAULT ""
func (this *Client) Alter(tableName, action string, fields []string) error {
	switch action {
	case "ADD", "DROP":
	default:
		return fmt.Errorf("action only ADD or DROP")
	}
	sqlStr := `
ALTER TABLE "public"."%s"`
	if len(fields) == 0 {
		return fmt.Errorf("fields length is 0")
	}
	tmp := `
%s COLUMN %s`
	columns := []string{}
	for _, field := range fields {
		columns = append(columns, fmt.Sprintf(tmp, action, field))
	}
	sqlStr += strings.Join(columns, ",")
	sqlStr += ";"

	sqlStatement := fmt.Sprintf(sqlStr, tableName)
	//fmt.Println(sqlStatement)
	smt, err := this.dbConn.Prepare(sqlStatement)
	if err != nil {
		return err
	}
	_, err = smt.Exec()
	return err
}

// 清空表
func (this *Client) Truncate(tableName string) error {
	sqlStr := `
TRUNCATE TABLE %s;`

	sqlStatement := fmt.Sprintf(sqlStr, tableName)
	//fmt.Println(sqlStatement)
	smt, err := this.dbConn.Prepare(sqlStatement)
	if err != nil {
		return err
	}
	_, err = smt.Exec()
	return err
}

// 表行数
func (this *Client) Count(tableName string) (int, error) {
	sqlStr := `
select count(*) from %s;`

	sqlStatement := fmt.Sprintf(sqlStr, tableName)
	smt, err := this.dbConn.Prepare(sqlStatement)
	if err != nil {
		return 0, err
	}
	row := smt.QueryRow()
	var count int
	err = row.Scan(&count)
	return count, err
}
