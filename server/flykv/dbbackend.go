package flykv

import (
	"fmt"
	"sync"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/sniperHW/flyfish/backend/db"
	"github.com/sniperHW/flyfish/backend/db/sql"
	sslot "github.com/sniperHW/flyfish/server/slot"
)

type dbbackendI interface {
	issueLoad(l db.DBLoadTask) bool
	issueUpdate(u db.DBUpdateTask) bool
	stop()
	start(config *Config) error
}

type sqlDbBackend struct {
	loaders  []db.DBLoader
	updaters []db.DBUpdater
	wait     sync.WaitGroup
}

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

func NewSqlDbBackend() *sqlDbBackend {
	return &sqlDbBackend{}
}

func (d *sqlDbBackend) start(config *Config) error {
	dbConfig := config.DBConfig

	for i := 0; i < 5; i++ {
		dbl, err := sqlOpen(config.DBType, dbConfig.Host, dbConfig.Port, dbConfig.DataDB, dbConfig.User, dbConfig.Password)
		if nil != err {
			return err
		}
		d.loaders = append(d.loaders, sql.NewLoader(dbl, 200, 5000))

		dbw, err := sqlOpen(config.DBType, dbConfig.Host, dbConfig.Port, dbConfig.DataDB, dbConfig.User, dbConfig.Password)
		if nil != err {
			return err
		}

		d.updaters = append(d.updaters, sql.NewUpdater(dbw, config.DBType, d.wait))
	}

	for i := 0; i < 5; i++ {
		d.loaders[i].Start()
		d.updaters[i].Start()
	}

	return nil
}

func (d *sqlDbBackend) issueLoad(l db.DBLoadTask) bool {
	idx := sslot.StringHash(l.GetUniKey())
	return d.loaders[idx%len(d.loaders)].IssueLoadTask(l) == nil
}

func (d *sqlDbBackend) issueUpdate(u db.DBUpdateTask) bool {
	idx := sslot.StringHash(u.GetUniKey())
	return d.updaters[idx%len(d.updaters)].IssueUpdateTask(u) == nil
}

func (d *sqlDbBackend) stop() {
	//等待所有updater结束
	d.wait.Wait()
}
