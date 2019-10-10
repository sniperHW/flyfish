package kvnode

import (
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/sniperHW/flyfish/conf"
	futil "github.com/sniperHW/flyfish/util"
	"github.com/sniperHW/flyfish/util/str"
	"github.com/sniperHW/kendynet/timer"
	"github.com/sniperHW/kendynet/util"
	"sync"
	"sync/atomic"
	"time"
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

type sqlPing struct {
}

type sqlMgr struct {
	sqlUpdateWg         *sync.WaitGroup
	sqlLoaders          []*sqlLoader
	sqlUpdaters         []*sqlUpdater
	stoped              int64
	totalUpdateSqlCount int64

	binaryToSqlStr          func(s *str.Str, bytes []byte)
	buildInsertUpdateString func(s *str.Str, kv *kv)
}

func (this *sqlMgr) pushLoadReq(task asynCmdTaskI, fullReturn ...bool) bool {
	l := this.sqlLoaders[futil.StringHash(task.getKV().uniKey)%len(this.sqlLoaders)]
	err := l.queue.AddNoWait(task, fullReturn...)
	if nil == err {
		return true
	} else {
		return false
	}
}

func (this *sqlMgr) pushUpdateReq(kv *kv) {
	u := this.sqlUpdaters[futil.StringHash(kv.uniKey)%len(this.sqlUpdaters)]
	u.queue.AddNoWait(kv)
}

func (this *sqlMgr) stop() {

	if atomic.CompareAndSwapInt64(&this.stoped, 0, 1) {

		for _, v := range this.sqlLoaders {
			v.queue.Close()
		}
		for _, v := range this.sqlUpdaters {
			v.queue.Close()
		}
		this.sqlUpdateWg.Wait()
	}
}

func (this *sqlMgr) isStoped() bool {
	return atomic.LoadInt64(&this.stoped) == 1
}

func newSqlMgr() (*sqlMgr, error) {
	config := conf.GetConfig()
	dbConfig := config.DBConfig

	sqlMgr := &sqlMgr{}

	if dbConfig.SqlType == "mysql" {
		sqlMgr.binaryToSqlStr = mysqlBinaryToPgsqlStr
		sqlMgr.buildInsertUpdateString = sqlMgr.buildInsertUpdateStringMySql
	} else {
		sqlMgr.binaryToSqlStr = pgsqlBinaryToPgsqlStr
		sqlMgr.buildInsertUpdateString = sqlMgr.buildInsertUpdateStringPgSql
	}

	sqlLoaders := []*sqlLoader{}

	ping := sqlPing{}

	for i := 0; i < config.SqlLoaderCount; i++ {
		lname := fmt.Sprintf("sqlLoad:%d", i)
		var loadDB *sqlx.DB
		var err error
		loadDB, err = sqlOpen(dbConfig.SqlType, dbConfig.DbHost, dbConfig.DbPort, dbConfig.DbDataBase, dbConfig.DbUser, dbConfig.DbPassword)

		if nil != err {
			return nil, err
		}
		l := newSqlLoader(loadDB, lname)
		sqlLoaders = append(sqlLoaders, l)
		go l.run()
		timer.Repeat(time.Second*60, nil, func(t *timer.Timer) {
			if sqlMgr.isStoped() || util.ErrQueueClosed == l.queue.AddNoWait(ping) {
				t.Cancel()
			}
		})
	}

	sqlUpdaters := []*sqlUpdater{}

	for i := 0; i < config.SqlUpdaterCount; i++ {
		wname := fmt.Sprintf("sqlUpdater:%d", i)
		var writeBackDB *sqlx.DB
		var err error
		writeBackDB, err = sqlOpen(dbConfig.SqlType, dbConfig.DbHost, dbConfig.DbPort, dbConfig.DbDataBase, dbConfig.DbUser, dbConfig.DbPassword)
		if nil != err {
			return nil, err
		}

		u := newSqlUpdater(sqlMgr, writeBackDB, wname)
		sqlUpdaters = append(sqlUpdaters, u)
		go u.run()
		timer.Repeat(time.Second*60, nil, func(t *timer.Timer) {
			if sqlMgr.isStoped() || util.ErrQueueClosed == u.queue.AddNoWait(ping) {
				t.Cancel()
			}
		})
	}

	sqlMgr.sqlUpdaters = sqlUpdaters
	sqlMgr.sqlLoaders = sqlLoaders

	return sqlMgr, nil
}

func loadMetaString() ([]string, error) {
	var db *sqlx.DB
	var err error
	dbConfig := conf.GetConfig().DBConfig

	db, err = sqlOpen(dbConfig.SqlType, dbConfig.ConfDbHost, dbConfig.ConfDbPort, dbConfig.ConfDataBase, dbConfig.ConfDbUser, dbConfig.ConfDbPassword)

	if nil != err {
		return nil, err
	} else {

		rows, err := db.Query("select __table__,__conf__ from table_conf")

		if nil != err {
			return nil, err
		}
		defer rows.Close()

		metaString := []string{}

		for rows.Next() {
			var __table__ string
			var __conf__ string

			err := rows.Scan(&__table__, &__conf__)

			if nil != err {
				return nil, err
			}

			metaString = append(metaString, fmt.Sprintf("%s@%s", __table__, __conf__))
		}

		return metaString, nil
	}

}
