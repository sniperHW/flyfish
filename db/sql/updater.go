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
	dbc *sqlx.DB
	//count     int
	que *queue.ArrayQueue
	//waitGroup *sync.WaitGroup
	//startOnce sync.Once
	sqlExec sqlExec
}

func (this *updater) IssueUpdateTask(t db.DBUpdateTask) error {
	return this.que.ForceAppend(t)
}

func (this *updater) Stop() {
	this.que.Close()
}

func abs(v int64) int64 {
	if v > 0 {
		return v
	} else {
		return 0 - v
	}
}

func getUpdateType(s db.DBState) string {
	switch s {
	case db.DBState_insert:
		return "insert"
	case db.DBState_update:
		return "update"
	case db.DBState_delete:
		return "delete"
	default:
		return "unknown"
	}
}

var errNewerWriteBackSuccess error = errors.New("a newer writeback success")
var errRecordNotExist error = errors.New("record not exist")

func (this *updater) exec(v interface{}) {
	switch v.(type) {
	case db.DBUpdateTask:
		task := v.(db.DBUpdateTask)
		if !task.HasPermission() {
			task.UpdateCallback(0, db.Err_NoPermission)
		} else {
			s := task.GetUpdateState()
			b := buffer.Get()
			defer b.Free()
			needRebuildSql := true
			for {
				if !task.HasPermission() {
					task.UpdateCallback(0, db.Err_NoPermission)
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
						task.UpdateCallback(0, db.Err_InvaildDbState)
						return
					}
				}

				loadVersion := func() (int64, error) {
					meta := s.Meta.(*TableMeta)
					for {
						if !task.HasPermission() {
							return 0, db.Err_NoPermission
						}

						rows, err := this.dbc.Query(fmt.Sprintf("select __version__ from %s where __key__ = '%s' for update;", meta.real_tableName, s.Key))
						defer rows.Close()
						if nil != err {
							if isRetryError(err) {
								//休眠一秒重试
								time.Sleep(time.Second)
							} else {
								return 0, err
							}
						} else {
							if rows.Next() {
								var version int64
								rows.Scan(&version)
								if abs(version) >= abs(s.Version) {
									//GetSugar().Errorf("a newer writeback success,drop old writeback db.version:%d,task.version:%d", version, s.Version)
									//数据库已经有最新的回写
									return version, errNewerWriteBackSuccess
								} else {
									return version, nil
								}
							} else {
								return 0, errRecordNotExist
							}
						}
					}

					return 0, nil
				}

				rowsAffected, err := this.sqlExec.exec(this.dbc)
				if nil == err {
					if rowsAffected > 0 {
						GetSugar().Debugf("%s ok last:%d version:%d unikey:%s:%s", getUpdateType(s.State), s.LastWriteBackVersion, s.Version, s.Meta.TableName(), s.Key)
						//回写成功
						task.UpdateCallback(s.Version, nil)
						return
					} else {
						GetSugar().Infof("%s version mismatch last:%d version:%d unikey:%s:%s", getUpdateType(s.State), s.LastWriteBackVersion, s.Version, s.Meta.TableName(), s.Key)
						//版本号不匹配，再次获取版本号
						version, err := loadVersion()
						if nil == err {
							GetSugar().Infof("RebuildSql %s version mismatch last:%d version:%d,dbversion:%d", getUpdateType(s.State), s.LastWriteBackVersion, s.Version, version)
							s.LastWriteBackVersion = version
							needRebuildSql = true
						} else if err == errRecordNotExist {
							s.State = db.DBState_insert
							needRebuildSql = true
						} else if err == errNewerWriteBackSuccess {
							/*
							 * 情况1：old leader执行回写前,新leader已经执行了更新的回写，old leader执行SetLastWriteBackVersion时会因为没有leader ship而失败。
							 * 情况2：数据库与flyfish数据发生不一致。例如数据库中已经存在数据，切版本号为2.而flyfish认为数据不存在，因此执行创建，版本号为1。发生此情况
							 * 在SetLastWriteBackVersion中应该记录日志。用数据中的version修正kv.version以及kv.LastWriteBackVersion。出现此情况必然导致数据丢失，
							 * 修正只能让程序继续正确运行。
							 */
							task.UpdateCallback(version, nil)
							return
						} else {
							task.UpdateCallback(0, err)
							return
						}
					}
				} else {
					GetSugar().Errorf("sqlUpdater exec %s %v", this.sqlExec.b.ToStrUnsafe(), err)
					if isRetryError(err) {
						//休眠一秒重试
						time.Sleep(time.Second)
					} else {
						task.UpdateCallback(0, err)
						return
					}
				}
			}
		}
	}
}

func NewUpdater(dbc *sqlx.DB, sqlType string, waitGroup *sync.WaitGroup) *updater {
	u := &updater{
		que: queue.NewArrayQueue(),
		dbc: dbc,
		sqlExec: sqlExec{
			sqlType: sqlType,
		},
	}
	if nil != waitGroup {
		waitGroup.Add(1)
	}
	go func() {
		localList := make([]interface{}, 0, 200)
		closed := false
		for {
			localList, closed = u.que.Pop(localList)
			size := len(localList)
			if closed && size == 0 {
				break
			}
			for i, v := range localList {
				u.exec(v)
				localList[i] = nil
			}
		}
		if nil != waitGroup {
			waitGroup.Done()
		}
	}()
	return u
}
