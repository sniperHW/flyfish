package server

import (
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/sniperHW/flyfish/conf"
	futil "github.com/sniperHW/flyfish/util"
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

	BinaryToSqlStr          func(s *str.Str, bytes []byte)
	buildInsertUpdateString func(s *str.Str, kv *kv)
}

func (this *sqlMgr) pushLoadReq(task asynCmdTaskI, fullReturn ...bool) bool {
	l := this.sqlLoaders[futil.StringHash(task.getKV().uniKey)%len(sqlLoaders)]
	err := l.queue.AddNoWait(task, fullReturn...)
	if nil == err {
		return true
	} else {
		return false
	}
}

func (this *sqlMgr) pushUpdateReq(kv *kv) {
	u := this.sqlUpdaters[futil.StringHash(kv.uniKey)%len(sqlUpdaters)]
	u.queue.AddNoWait(ckey)
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

func (this *sqlMgr) isStoped() {
	return atomic.LoadInt64(&this.stoped)
}

func newSqlMgr() (*sqlMgr, error) {
	config := conf.GetConfig()
	dbConfig := config.DBConfig

	sqlMgr := &sqlMgr{}

	if dbConfig.SqlType == "mysql" {
		sqlMgr.BinaryToSqlStr = sqlMgr.mysqlBinaryToPgsqlStr
		sqlMgr.buildInsertUpdateString = sqlMgr.buildInsertUpdateStringMySql
	} else {
		sqlMgr.BinaryToSqlStr = sqlMgr.pgsqlBinaryToPgsqlStr
		sqlMgr.buildInsertUpdateString = sqlMgr.buildInsertUpdateStringPgSql
	}

	sqlLoaders = []*sqlLoader{}
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
			if sqlMgr.isStoped() || util.ErrQueueClosed == l.queue.AddNoWait(sqlPing) {
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
			return nil, err
		}

		u := newSqlUpdater(sqlMgr, writeBackDB, wname)
		sqlUpdaters = append(sqlUpdaters, u)
		go u.run()
		timer.Repeat(time.Second*60, nil, func(t *timer.Timer) {
			if sqlMgr.isStoped() || util.ErrQueueClosed == u.queue.AddNoWait(sqlPing) {
				t.Cancel()
			}
		})
	}

	sqlMgr.sqlUpdaters = sqlUpdaters
	sqlMgr.sqlLoaders = sqlLoaders

	return sqlMgr, nil
}
