package sql

import (
	"database/sql/driver"
	"github.com/jmoiron/sqlx"
	"github.com/sniperHW/flyfish/backend/db"
	"github.com/sniperHW/flyfish/pkg/buffer"
	"github.com/sniperHW/flyfish/pkg/queue"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

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

type updater struct {
	dbc       *sqlx.DB
	lastTime  time.Time
	count     int
	que       *queue.ArrayQueue
	waitGroup sync.WaitGroup
	stoped    int32
	stoponce  sync.Once
	toSqlStr  sqlstring
}

func (this *updater) IssueUpdateTask(t db.DBUpdateTask) error {
	return this.que.ForceAppend(t)
}

func (this *updater) isStoped() bool {
	return atomic.LoadInt32(&this.stoped) == 1
}

func (this *updater) Stop() {
	this.stoponce.Do(func() {
		this.que.Close()
		atomic.StoreInt32(&this.stoped, 1)
	})
}

func (this *updater) Start() {
	this.waitGroup.Add(1)
	go func() {
		for {
			time.Sleep(time.Second * 60)
			if nil != this.que.ForceAppend(sqlping) {
				return
			}
		}
	}()

	go func() {
		defer this.waitGroup.Done()
		localList := make([]interface{}, 0, 200)
		closed := false
		for {

			localList, closed = this.que.Pop(localList)
			size := len(localList)
			if closed && size == 0 {
				break
			}

			for i, v := range localList {
				this.exec(v)
				localList[i] = nil
			}
		}
	}()
}

func (this *updater) exec(v interface{}) {

	switch v.(type) {
	case ping:
		if time.Now().Sub(this.lastTime) > time.Second*5*60 {
			//空闲超过5分钟发送ping
			err := this.dbc.Ping()
			if nil != err {
				GetSugar().Errorf("ping error %v\n", err)
			}
			this.lastTime = time.Now()
		}
	case db.DBUpdateTask:
		task := v.(db.DBUpdateTask)
		//检查是否还持有更新租约
		if !task.CheckUpdateLease() {
			task.ClearUpdateStateAndReleaseLock() //释放更新锁定
		} else {

			s := task.GetUpdateAndClearUpdateState()

			b := buffer.Get()
			defer b.Free()

			//构造更新语句
			switch s.State {
			case db.DBState_insert:
				this.toSqlStr.insertUpdateStatement(b, &s)
			case db.DBState_update:
				this.toSqlStr.updateStatement(b, &s)
			case db.DBState_delete:
				this.toSqlStr.deleteStatement(b, &s)
			default:
				GetSugar().Errorf("invaild dbstate %s %d", task.GetUniKey(), s.State)
				if task.Dirty() {
					//再次发生变更,插入队列继续执行
					this.que.ForceAppend(task)
				} else {
					task.ReleaseLock()
				}
				return
			}

			var err error

			for {
				str := b.ToStrUnsafe()
				_, err = this.dbc.Exec(str)
				if nil == err {
					break
				} else {
					GetSugar().Errorf("%s %v", str, err)
					if this.isStoped() || !task.CheckUpdateLease() {
						task.ReleaseLock()
						return
					} else {
						if isRetryError(err) {
							//休眠一秒重试
							time.Sleep(time.Second)
						} else {
							GetSugar().Errorf("sqlUpdater exec error:%v", err)
							break
						}
					}
				}
			}

			if task.Dirty() {
				//再次发生变更,插入队列继续执行
				this.que.ForceAppend(task)
			} else {
				task.ReleaseLock()
			}
		}
	}
}

func NewUpdater(dbc *sqlx.DB, sqlType string, waitGroup sync.WaitGroup) *updater {
	u := &updater{
		que:       queue.NewArrayQueue(),
		dbc:       dbc,
		waitGroup: waitGroup,
	}

	if sqlType == "mysql" {
		u.toSqlStr = sqlstring{
			binarytostr: binaryTomySqlStr,
		}
		u.toSqlStr.buildInsertUpdateString = u.toSqlStr.insertUpdateStatementMySql
	} else {
		u.toSqlStr = sqlstring{
			binarytostr: binaryTopgSqlStr,
		}
		u.toSqlStr.buildInsertUpdateString = u.toSqlStr.insertUpdateStatementPgSql
	}
	return u
}
