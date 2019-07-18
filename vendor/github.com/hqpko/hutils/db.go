package hutils

import "fmt"

func NewDataSourceName(user, password, host, port, dbName string) string {
	return fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?parseTime=true&charset=utf8&loc=Local", user, password, host, port, dbName)
}
