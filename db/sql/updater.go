package sql

import (
	"database/sql/driver"
	"errors"
	"fmt"
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
		if nil != this.waitGroup {
			this.waitGroup.Add(1)
		}
		go func() {
			if nil != this.waitGroup {
				defer this.waitGroup.Done()
			}
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

func abs(v int64) int64 {
	if v > 0 {
		return v
	} else {
		return 0 - v
	}
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

			needRebuildSql := true

			for {

				if !task.CheckUpdateLease() {
					task.ClearUpdateStateAndReleaseLock()
					return
				}

				if needRebuildSql {
					b.Reset()

					//构造更新语句
					switch s.State {
					case db.DBState_insert:
						this.sqlExec.prepareInsertUpdate(b, &s)
					case db.DBState_update:
						this.sqlExec.prepareUpdate(b, &s)
					case db.DBState_delete:
						this.sqlExec.prepareMarkDelete(b, &s)
					default:
						GetSugar().Errorf("invaild dbstate %s %d", task.GetUniKey(), s.State)
						if task.Dirty() && task.CheckUpdateLease() {
							//再次发生变更,插入队列继续执行
							this.que.ForceAppend(task)
						} else {
							task.ReleaseLock()
						}
						return
					}
				}

				loadVersion := func() (int64, error) {
					meta := s.Meta.(*TableMeta)
					for {
						if !task.CheckUpdateLease() {
							return 0, errors.New("no lease")
						}

						rows, err := this.dbc.Query(fmt.Sprintf("select __version__ from %s where __key__ = '%s';", meta.real_tableName, s.Key))
						if nil != err {
							if isRetryError(err) {
								//休眠一秒重试
								time.Sleep(time.Second)
							} else {
								return 0, errors.New("drop writeback")
							}
						} else {
							if rows.Next() {
								var version int64
								rows.Scan(&version)
								if abs(version) >= abs(s.Version) {
									//数据库已经有最新的回写
									return 0, errors.New("drop writeback")
								} else {
									return version, nil
								}
							} else {
								return 0, errors.New("drop writeback")
							}
						}
					}

					return 0, nil
				}

				rowsAffected, err := this.sqlExec.exec(this.dbc)
				if nil == err {
					if rowsAffected > 0 {
						//回写成功
						task.SetLastWriteBackVersion(s.Version)
						break
					} else {
						//版本号不匹配，再次获取版本号
						if version, err := loadVersion(); nil != err {
							break
						} else {
							s.LastWriteBackVersion = version
							needRebuildSql = true
						}
					}
				} else {
					GetSugar().Errorf("sqlUpdater exec %s %v", this.sqlExec.b.ToStrUnsafe(), err)
					if isRetryError(err) {
						//休眠一秒重试
						time.Sleep(time.Second)
					} else {
						break
					}
				}
			}

			if task.Dirty() && task.CheckUpdateLease() {
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
