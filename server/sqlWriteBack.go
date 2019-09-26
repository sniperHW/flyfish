package server

import (
	"database/sql/driver"
	"fmt"
	"github.com/jmoiron/sqlx"
	"github.com/sniperHW/kendynet/util"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

type sqlUpdater struct {
	db        *sqlx.DB
	name      string
	lastTime  time.Time
	queue     *util.BlockQueue
	wg        *sync.WaitGroup
	sqlStr    *str
	server    *Server
	localList []interface{}
}

func newSqlUpdater(server *Server, db *sqlx.DB, name string, wg *sync.WaitGroup) *sqlUpdater {
	if nil != wg {
		wg.Add(1)
	}
	return &sqlUpdater{
		name:      name,
		queue:     util.NewBlockQueueWithName(name),
		db:        db,
		wg:        wg,
		server:    server,
		localList: []interface{}{},
	}
}

func isRetryError(err error) bool {
	if err == driver.ErrBadConn {
		return true
	} else {
		switch err.(type) {
		case *net.OpError:
			return true
		case net.Error:
			return true
		default:
		}
	}
	return false
}

var totalSqlCount int64

func (this *sqlUpdater) reset() {
	this.sqlStr.reset()
}

func (this *sqlUpdater) onSqlResult(ckey *cacheKey, err error) {
	ckey.mtx.Lock()
	defer ckey.mtx.Unlock()
	if err == errLoseLease {
		ckey.writeBackLocked = false
	} else if err != errServerStop {
		if write_back_none == ckey.sqlFlag {
			ckey.writeBackLocked = false
		} else {
			//执行exec时再次发生变更
			this.localList = append(this.localList)
		}
	}
}

var (
	errServerStop = fmt.Errorf("errServerStop")
	errLoseLease  = fmt.Errorf("errLoseLease")
)

func (this *sqlUpdater) exec(v interface{}) {

	defer this.reset()

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

		rn := ckey.store.rn

		if !rn.l.hasLease(rn) {
			//没有持有租约,不能执行sql操作。
			ckey.mtx.Lock()
			ckey.writeBackLocked = false
			ckey.mtx.Unlock()
			return
		}

		Debugln(*ckey)

		meta := ckey.getMeta()

		if nil == meta {
			Fatalln("invaild ctx")
		}

		ckey.mtx.Lock()

		tt := ckey.sqlFlag
		if tt == write_back_insert || tt == write_back_insert_update {
			buildInsertUpdateString(this.sqlStr, ckey)
			atomic.AddInt64(&totalSqlCount, 1)
		} else if tt == write_back_update {
			buildUpdateString(this.sqlStr, ckey)
			atomic.AddInt64(&totalSqlCount, 1)
		} else if tt == write_back_delete {
			buildDeleteString(this.sqlStr, ckey)
			atomic.AddInt64(&totalSqlCount, 1)
		}

		ckey.sqlFlag = write_back_none

		if len(ckey.modifyFields) > 0 {
			ckey.modifyFields = map[string]bool{}
		}

		ckey.mtx.Unlock()

		var err error

		for {
			_, err = this.db.Exec(this.sqlStr.toString())
			if nil == err {
				break
			} else {
				Errorln(this.sqlStr.toString(), err)
				if isRetryError(err) {
					Errorln("sqlUpdater exec error:", err)
					if this.server.isStoped() {
						err = errServerStop
						break
					}

					if !rn.l.hasLease(rn) {
						//已经失去租约，不能再执行
						err = errLoseLease
						break
					}

					//休眠一秒重试
					time.Sleep(time.Second)
				} else {
					Errorln("sqlUpdater exec error:", err)
					break
				}
			}
		}

		this.onSqlResult(ckey, err)
	}
}

func (this *sqlUpdater) run() {
	for {
		closed, localList := this.queue.Get()
		for _, v := range localList {
			this.exec(v)
		}

		for {
			if len(this.localList) > 0 {
				localList = this.localList
				this.localList = []interface{}{}
				for _, v := range localList {
					this.exec(v)
				}
			} else {
				break
			}

			if this.queue.Len() > 0 || len(this.localList) == 0 {
				break
			}
		}

		if closed {
			Infoln(this.name, "stoped")
			if nil != this.wg {
				this.wg.Done()
			}
			return
		}
	}
}
