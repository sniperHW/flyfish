package metaLoader

import (
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	backenddb "github.com/sniperHW/flyfish/db"
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

func LoadDBMetaFromSqlJson(sqlType string, host string, port int, dbname string, user string, password string) (*backenddb.DbDef, error) {
	var db *sqlx.DB
	var err error
	var def *backenddb.DbDef

	db, err = sqlOpen(sqlType, host, port, dbname, user, password)

	if nil != err {
		return nil, err
	} else {

		rows, err := db.Query("select dbmeta from dbmeta")

		if nil != err {
			return nil, err
		}
		defer rows.Close()

		var meta string

		if rows.Next() {
			err := rows.Scan(&meta)
			if nil != err {
				return nil, err
			}
		}

		def, err = backenddb.CreateDbDefFromJsonString([]byte(meta))
		return def, err
	}
}

//每个表一行定义
func LoadDBMetaFromSqlCsv(sqlType string, host string, port int, dbname string, user string, password string) (*backenddb.DbDef, error) {
	db, err := sqlOpen(sqlType, host, port, dbname, user, password)
	if nil != err {
		return nil, err
	} else {

		rows, err := db.Query("select __table__,__conf__ from table_conf")

		if nil != err {
			return nil, err
		}

		defer rows.Close()

		strs := []string{}

		for rows.Next() {
			var __table__ string
			var __conf__ string

			err := rows.Scan(&__table__, &__conf__)

			if nil != err {
				return nil, err
			}

			strs = append(strs, __table__+"@"+__conf__)
		}

		return backenddb.CreateDbDefFromCsv(strs)
	}

}
