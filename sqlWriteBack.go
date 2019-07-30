package flyfish

import (
	"database/sql/driver"
	"flyfish/conf"
	"flyfish/errcode"
	"github.com/jmoiron/sqlx"
	"github.com/sniperHW/kendynet/util"
	"net"
	"sync"
	"time"
)

type sqlUpdater struct {
	db                *sqlx.DB
	name              string
	values            []interface{}
	writeFileAndBreak bool
	lastTime          time.Time
	queue             *util.BlockQueue
	wg                *sync.WaitGroup
}

func newSqlUpdater(db *sqlx.DB, name string, wg *sync.WaitGroup) *sqlUpdater {
	if nil != wg {
		wg.Add(1)
	}
	return &sqlUpdater{
		name:   name,
		values: []interface{}{},
		queue:  util.NewBlockQueueWithName(name, conf.GetConfig().SqlUpdateQueueSize),
		db:     db,
		wg:     wg,
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

func (this *sqlUpdater) resetValues() {
	this.values = this.values[0:0]
}

func (this *sqlUpdater) doInsert(r *writeBackRecord, meta *table_meta) error {
	str := strGet()
	defer func() {
		this.resetValues()
		strPut(str)
	}()

	str.append(meta.insertPrefix).append(getInsertPlaceHolder(1)).append(getInsertPlaceHolder(2))
	this.values = append(this.values, r.key, r.fields["__version__"].GetValue())
	c := 2
	for _, name := range meta.insertFieldOrder {
		c++
		str.append(getInsertPlaceHolder(c))
		this.values = append(this.values, r.fields[name].GetValue())
	}
	str.append(");")
	_, err := this.db.Exec(str.toString(), this.values...)

	return err
}

func (this *sqlUpdater) doUpdate(r *writeBackRecord) error {

	str := strGet()
	defer func() {
		this.resetValues()
		strPut(str)
	}()

	str.append("update ").append(r.table).append(" set ")
	i := 0
	for _, v := range r.fields {
		this.values = append(this.values, v.GetValue())
		i++
		if i == 1 {
			str.append(v.GetName()).append("=").append(getUpdatePlaceHolder(i))
		} else {
			str.append(",").append(v.GetName()).append("=").append(getUpdatePlaceHolder(i))
		}
	}
	str.append(" where __key__ = '").append(r.key).append("';")
	_, err := this.db.Exec(str.toString(), this.values...)
	return err
}

func (this *sqlUpdater) doDelete(r *writeBackRecord) error {
	str := strGet()
	defer strPut(str)
	str.append("delete from ").append(r.table).append(" where __key__ = '").append(r.key).append("';")
	_, err := this.db.Exec(str.toString())
	return err
}

/*func updateDefer(r *writeBackRecord) {
	r.ckey.clearWriteBack()
	recordPut(r)
}*/

func (this *sqlUpdater) process(v interface{}) {

	if _, ok := v.(*processContext); ok {
		if time.Now().Sub(this.lastTime) > time.Second*5*60 {
			//空闲超过5分钟发送ping
			err := this.db.Ping()
			if nil != err {
				Errorln("sqlUpdater ping error", err)
			} else {
				Debugln("sqlUpdater ping")
			}
			this.lastTime = time.Now()
		}
	} else {

		this.lastTime = time.Now()

		wb := v.(*cacheKey).getRecord()

		defer wb.ckey.clearWriteBack()

		if this.writeFileAndBreak {
			backupRecord(wb)
			return
		}

		var err error

		for {

			if wb.writeBackFlag == write_back_update {
				err = this.doUpdate(wb)
			} else if wb.writeBackFlag == write_back_insert {
				err = this.doInsert(wb, wb.ckey.getMeta())
			} else if wb.writeBackFlag == write_back_delete {
				err = this.doDelete(wb)
			} else {
				return
			}

			if nil == err {
				if wb.ctx != nil {
					sqlResponse.onSqlWriteBackResp(wb.ctx, errcode.ERR_OK)
				}
				return
			} else {
				if isRetryError(err) {
					Errorln("sqlUpdater exec error:", err)
					if isStop() {
						this.writeFileAndBreak = true
						backupRecord(wb)
						return
					}
					//休眠一秒重试
					time.Sleep(time.Second)
				} else {
					if wb.ctx != nil {
						sqlResponse.onSqlWriteBackResp(wb.ctx, errcode.ERR_SQLERROR)
					}
					Errorln("sqlUpdater exec error:", err)
					return
				}
			}
		}
	}
}

func (this *sqlUpdater) run() {
	for {
		closed, localList := this.queue.Get()
		for _, v := range localList {
			this.process(v)
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
