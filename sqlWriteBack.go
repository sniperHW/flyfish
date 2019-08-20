package flyfish

import (
	"database/sql/driver"
	"github.com/jmoiron/sqlx"
	//"github.com/sniperHW/flyfish/errcode"
	"github.com/sniperHW/kendynet/util"
	"net"
	"sync"
	"time"
)

type sqlUpdater struct {
	db       *sqlx.DB
	name     string
	lastTime time.Time
	queue    *util.BlockQueue
	wg       *sync.WaitGroup
	sqlStr   *str
	max      int
	count    int
}

func newSqlUpdater(db *sqlx.DB, name string, wg *sync.WaitGroup) *sqlUpdater {
	if nil != wg {
		wg.Add(1)
	}
	return &sqlUpdater{
		name:  name,
		queue: util.NewBlockQueueWithName(name),
		db:    db,
		wg:    wg,
		max:   200,
		count: 0,
	}
}

func isRetryError(err error) bool {
	if err == driver.ErrBadConn {
		return true
	} else {
		switch err.(type) {
		case *net.OpError:
			return true
			break
		case net.Error:
			return true
			break
		default:
			break
		}
	}
	return false
}

func (this *sqlUpdater) append(v interface{}) {
	if v == nil {
		if time.Now().Sub(this.lastTime) > time.Second*5*60 {
			//空闲超过5分钟发送ping
			err := this.db.Ping()
			if nil != err {
				Errorln("ping error", err)
			}
			this.lastTime = time.Now()
		}
	} else {
		if nil == this.sqlStr {
			this.sqlStr = &str{
				data: make([]byte, strThreshold),
				cap:  strThreshold,
				len:  0,
			}
		}

		ckey := v.(*cacheKey)

		Debugln(*ckey)

		meta := ckey.getMeta()

		ckey.mtx.Lock()

		tt := ckey.sqlFlag
		if tt == write_back_insert || tt == write_back_insert_update {
			buildInsertUpdateString(this.sqlStr, ckey)
		} else if tt == write_back_update {
			buildUpdateString(this.sqlStr, ckey)
		} else if tt == write_back_delete {
			buildDeleteString(this.sqlStr, ckey)
		} else {
			Fatalln("invaild ctx")
		}

		ckey.writeBackLocked = false
		ckey.sqlFlag = write_back_none

		if len(ckey.modifyFields) > 0 {
			ckey.modifyFields = map[string]bool{}
		}

		ckey.mtx.Unlock()

		if nil == meta {
			Fatalln("invaild ctx")
		}

		this.count++

		if this.count >= this.max {
			this.exec()
		}
	}
}

func (this *sqlUpdater) reset() {
	this.count = 0
	this.sqlStr.reset()
}

func (this *sqlUpdater) exec() {
	if this.count > 0 {

		defer this.reset()

		//beg := time.Now()

		for {
			_, err := this.db.Exec(this.sqlStr.toString())
			if nil == err {
				//Infoln("writeback time:", time.Now().Sub(beg), "record:", this.count)
				break
			} else {
				Errorln(this.sqlStr.toString(), err)
				if isRetryError(err) {
					Errorln("sqlUpdater exec error:", err)
					if isStop() {
						return
					}
					//休眠一秒重试
					time.Sleep(time.Second)
				} else {
					Errorln("sqlUpdater exec error:", err)
					break
				}
			}
		}
	}
}

func (this *sqlUpdater) run() {
	for {
		closed, localList := this.queue.Get()
		for _, v := range localList {
			this.append(v)
		}
		this.exec()
		if closed {
			Infoln(this.name, "stoped")
			if nil != this.wg {
				this.wg.Done()
			}
			return
		}
	}
}
