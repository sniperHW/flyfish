package server

import (
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/sniperHW/flyfish/conf"
	"github.com/sniperHW/kendynet/timer"
	"github.com/sniperHW/kendynet/util"
	"sync"
	"time"
)

var (
	sqlUpdateWg *sync.WaitGroup
	sqlLoaders  []*sqlLoader
	sqlUpdaters []*sqlUpdater
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

func pushSqlLoadReq(ctx *cmdContext, fullReturn ...bool) bool {
	l := sqlLoaders[StringHash(ctx.getUniKey())%len(sqlLoaders)]
	err := l.queue.AddNoWait(ctx, fullReturn...)
	if nil == err {
		return true
	} else {
		return false
	}
}

func pushSqlWriteReq(ckey *cacheKey) {
	u := sqlUpdaters[StringHash(ckey.uniKey)%len(sqlUpdaters)]
	u.queue.AddNoWait(ckey)
}

func stopSql() {
	for _, v := range sqlUpdaters {
		v.queue.Close()
	}
	sqlUpdateWg.Wait()
}

func updateSqlUpdateQueueSize(SqlUpdateQueueSize int) {
	for _, v := range sqlUpdaters {
		v.queue.SetFullSize(SqlUpdateQueueSize)
	}
}

func updateSqlLoadQueueSize(SqlLoadQueueSize int) {
	for _, v := range sqlLoaders {
		v.queue.SetFullSize(SqlLoadQueueSize)
	}
}

func initSql(server *Server) bool {

	sqlUpdateWg = &sync.WaitGroup{}
	config := conf.GetConfig()
	dbConfig := config.DBConfig

	sqlLoaders = []*sqlLoader{}
	for i := 0; i < config.SqlLoaderCount; i++ {
		lname := fmt.Sprintf("sqlLoad:%d", i)
		var loadDB *sqlx.DB
		var err error
		loadDB, err = sqlOpen(dbConfig.SqlType, dbConfig.DbHost, dbConfig.DbPort, dbConfig.DbDataBase, dbConfig.DbUser, dbConfig.DbPassword)

		if nil != err {
			Errorln(err)
			return false
		}
		l := newSqlLoader(loadDB, lname)
		sqlLoaders = append(sqlLoaders, l)
		go l.run()
		timer.Repeat(time.Second*60, nil, func(t *timer.Timer) {
			if server.isStoped() || util.ErrQueueClosed == l.queue.AddNoWait(&cmdContext{ping: true}) {
				t.Cancel()
			}
		})
	}

	sqlUpdaters = []*sqlUpdater{}

	for i := 0; i < config.SqlUpdaterCount; i++ {
		wname := fmt.Sprintf("sqlUpdater:%d", i)
		var writeBackDB *sqlx.DB
		var err error
		writeBackDB, err = sqlOpen(dbConfig.SqlType, dbConfig.DbHost, dbConfig.DbPort, dbConfig.DbDataBase, dbConfig.DbUser, dbConfig.DbPassword)
		if nil != err {
			Errorln(err)
			return false
		}

		u := newSqlUpdater(server, writeBackDB, wname, sqlUpdateWg)
		sqlUpdaters = append(sqlUpdaters, u)
		go u.run()
		timer.Repeat(time.Second*60, nil, func(t *timer.Timer) {
			if server.isStoped() || util.ErrQueueClosed == u.queue.AddNoWait(nil) {
				t.Cancel()
			}
		})

	}

	return true

}
