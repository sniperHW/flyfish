package flyfish

import (
	"github.com/jmoiron/sqlx"
	//"database/sql"
	//"database/sql/driver"
	"fmt"
	_ "github.com/lib/pq"
	//"os"
)

func pgOpen(host string, port int, dbname string, user string, password string) (*sqlx.DB, error) {
	connStr := fmt.Sprintf("host=%s port=%d dbname=%s user=%s password=%s sslmode=disable", host, port, dbname, user, password)
	return sqlx.Open("postgres", connStr)
}
