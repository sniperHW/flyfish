package flyfish

import (
	"database/sql"
	//"database/sql/driver"
	_ "github.com/lib/pq"
	"fmt"
	//"os"
)

func pgOpen(dbname string,user string,password string) (*sql.DB,error) {
	connStr := fmt.Sprintf("dbname=%s user=%s password=%s sslmode=disable",dbname,user,password)
	return sql.Open("postgres", connStr)
}
