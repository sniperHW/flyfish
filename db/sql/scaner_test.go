package sql

//go test -covermode=count -v -coverprofile=coverage.out -run=TestScaner
//go tool cover -html=coverage.out
/*import (
	"fmt"
	"github.com/jmoiron/sqlx"
	"github.com/sniperHW/flyfish/db"
	"github.com/stretchr/testify/assert"
	"testing"
)

func pgsqlOpen(host string, port int, dbname string, user string, password string) (*sqlx.DB, error) {
	connStr := fmt.Sprintf("host=%s port=%d dbname=%s user=%s password=%s sslmode=disable", host, port, dbname, user, password)
	return sqlx.Open("postgres", connStr)
}

func mysqlOpen(host string, port int, dbname string, user string, password string) (*sqlx.DB, error) {
	connStr := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", user, password, host, port, dbname)
	return sqlx.Open("mysql", connStr)
}

func sqlOpen(sqlType string, host string, port int, dbname string, user string, password string) (*sqlx.DB, error) {
	if sqlType == "mysql" {
		return mysqlOpen(host, port, dbname, user, password)
	} else {
		return pgsqlOpen(host, port, dbname, user, password)
	}
}

func TestScaner(t *testing.T) {
	dbdef, err := db.CreateDbDefFromCsv([]string{"users1@name:string:,age:int:,phone:string:"})
	assert.Nil(t, err)
	meta, err := CreateDbMeta(1, dbdef)
	assert.Nil(t, err)
	dbc, err := sqlOpen("pgsql", "localhost", 5432, "test", "sniper", "123456")
	assert.Nil(t, err)

	scaner, err := NewScanner(meta.GetTableMeta("users1"), dbc, 2, "users1", []string{"name"}, []string{"huangwei:3", "sniper"})
	assert.Nil(t, err)

	rows, err := scaner.Next(10)
	assert.Nil(t, err)

	for _, v := range rows {
		fmt.Println(v.Key, v.Version, v.Fields)
	}

}*/
