package flykv

import (
	//"fmt"
	"github.com/jmoiron/sqlx"
	"github.com/sniperHW/flyfish/db"
	"github.com/sniperHW/flyfish/db/sql"
	sslot "github.com/sniperHW/flyfish/server/slot"
	"sync"
)

type dbI interface {
	issueLoad(l db.DBLoadTask) bool
	issueUpdate(u db.DBUpdateTask) bool
	stop()
	start(config *Config, dbc *sqlx.DB) error
}

type sqlDB struct {
	loaders  []db.DBLoader
	updaters []db.DBUpdater
	wait     sync.WaitGroup
}

func NewSqlDB() *sqlDB {
	return &sqlDB{}
}

func (d *sqlDB) start(config *Config, dbc *sqlx.DB) error {
	for i := 0; i < config.SqlLoaderCount; i++ {
		l := sql.NewLoader(dbc, 200, 5000)
		d.loaders = append(d.loaders, l)
		l.Start()
	}

	for i := 0; i < config.SqlLoaderCount; i++ {
		w := sql.NewUpdater(dbc, config.DBType, &d.wait)
		d.updaters = append(d.updaters, w)
		w.Start()
	}

	return nil
}

func (d *sqlDB) issueLoad(l db.DBLoadTask) bool {
	idx := sslot.StringHash(l.GetUniKey())
	return d.loaders[idx%len(d.loaders)].IssueLoadTask(l) == nil
}

func (d *sqlDB) issueUpdate(u db.DBUpdateTask) bool {
	idx := sslot.StringHash(u.GetUniKey())
	return d.updaters[idx%len(d.updaters)].IssueUpdateTask(u) == nil
}

func (d *sqlDB) stop() {
	for _, v := range d.loaders {
		v.Stop()
	}

	for _, v := range d.updaters {
		v.Stop()
	}

	//等待所有updater结束
	d.wait.Wait()
}
