package sql

import (
	"database/sql/driver"
	"github.com/jmoiron/sqlx"
	"github.com/sniperHW/flyfish/db"
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
	count     int
	que       *queue.ArrayQueue
	waitGroup *sync.WaitGroup
	stoped    int32
	startOnce sync.Once
	sqlExec   sqlExec
}

func (this *updater) IssueUpdateTask(t db.DBUpdateTask) error {
	return this.que.ForceAppend(t)
}

func (this *updater) Stop() {
	if atomic.CompareAndSwapInt32(&this.stoped, 0, 1) {
		this.que.Close()
	}
}

func (this *updater) Start() {
	this.startOnce.Do(func() {
		this.waitGroup.Add(1)
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
	})
}

func (this *updater) exec(v interface{}) {

	switch v.(type) {
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
				this.sqlExec.prepareInsertUpdate(b, &s)
			case db.DBState_update:
				this.sqlExec.prepareUpdate(b, &s)
			case db.DBState_delete:
				this.sqlExec.prepareDelete(b, &s)
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
				err = this.sqlExec.exec(this.dbc)
				if nil == err {
					break
				} else {
					GetSugar().Errorf("sqlUpdater exec %s %v", this.sqlExec.b.ToStrUnsafe(), err)
					if !task.CheckUpdateLease() {
						task.ClearUpdateStateAndReleaseLock()
						return
					} else {
						if isRetryError(err) {
							//休眠一秒重试
							time.Sleep(time.Second)
						} else {
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

func NewUpdater(dbc *sqlx.DB, sqlType string, waitGroup *sync.WaitGroup) *updater {
	return &updater{
		que:       queue.NewArrayQueue(),
		dbc:       dbc,
		waitGroup: waitGroup,
		sqlExec: sqlExec{
			sqlType: sqlType,
		},
	}
}
