package flyfish

import (
	"fmt"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
)

func mysqlOpen(host string, port int, dbname string, user string, password string) (*sqlx.DB, error) {
	//"root:meddeex@tcp(127.0.0.1:3306)/medex
	//connStr := fmt.Sprintf("host=%s port=%d dbname=%s user=%s password=%s", host, port, dbname, user, password)
	connStr := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", user, password, host, port, dbname)
	return sqlx.Open("mysql", connStr)
}
