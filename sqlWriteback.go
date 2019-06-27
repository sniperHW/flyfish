package flyfish

import (
	"database/sql/driver"
	"flyfish/conf"
	"flyfish/errcode"
	"flyfish/proto"
	"github.com/jmoiron/sqlx"
	"net"
	"sync"
	"time"
)

type writeBackBarrier struct {
	counter int
	waited  int
	mtx     sync.Mutex
	cond    *sync.Cond
}

func (this *writeBackBarrier) add() {
	this.mtx.Lock()
	this.counter++
	this.mtx.Unlock()
}

func (this *writeBackBarrier) sub(c int) {
	this.mtx.Lock()
	this.counter = this.counter - c
	if this.counter < conf.WriteBackEventQueueSize && this.waited > 0 {
		this.mtx.Unlock()
		this.cond.Broadcast()
	} else {
		this.mtx.Unlock()
	}
}

func (this *writeBackBarrier) wait() {
	defer this.mtx.Unlock()
	this.mtx.Lock()
	if this.counter >= conf.WriteBackEventQueueSize {
		for this.counter >= conf.WriteBackEventQueueSize {
			this.waited++
			this.cond.Wait()
			this.waited--
		}
	}
}

type record struct {
	writeBackFlag int
	key           string
	table         string
	uniKey        string
	ckey          *cacheKey
	fields        map[string]*proto.Field //所有命令的字段聚合
	expired       int64
	writeBackVer  int64
	ctx           *processContext
	droped        bool //已经丢弃
}

var recordPool = sync.Pool{
	New: func() interface{} {
		return &record{}
	},
}

func recordGet() *record {
	r := recordPool.Get().(*record)
	return r
}

func recordPut(r *record) {
	r.writeBackFlag = write_back_none
	r.ckey = nil
	r.fields = nil
	r.ctx = nil
	recordPool.Put(r)
}

type sqlUpdater struct {
	db                *sqlx.DB
	name              string
	values            []interface{}
	writeFileAndBreak bool
	lastTime          time.Time
}

func newSqlUpdater(name string, db *sqlx.DB) *sqlUpdater {
	return &sqlUpdater{
		name:   name,
		values: []interface{}{},
		db:     db,
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

func (this *sqlUpdater) exec() {

}

func (this *sqlUpdater) resetValues() {
	this.values = this.values[0:0]
}

func (this *sqlUpdater) doInsert(r *record, meta *table_meta) error {
	str := strGet()
	defer func() {
		this.resetValues()
		strPut(str)
	}()

	str.append(meta.insertPrefix).append(getInsertPlaceHolder(1)).append(getInsertPlaceHolder(2)) //append("($1,$2")
	this.values = append(this.values, r.key, r.fields["__version__"].GetValue())
	c := 2
	for _, name := range meta.insertFieldOrder {
		c++
		//str.append(fmt.Sprintf(",$%d", c))
		str.append(getInsertPlaceHolder(c))
		this.values = append(this.values, r.fields[name].GetValue())
	}
	str.append(");")
	_, err := this.db.Exec(str.toString(), this.values...)

	return err
}

func (this *sqlUpdater) doUpdate(r *record) error {

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
			//str.append(fmt.Sprintf(" %s = $%d", v.GetName(), i))
			str.append(v.GetName()).append("=").append(getUpdatePlaceHolder(i))
		} else {
			str.append(",").append(v.GetName()).append("=").append(getUpdatePlaceHolder(i))
			//str.append(fmt.Sprintf(",%s = $%d", v.GetName(), i))
		}
	}
	str.append(" where __key__ = '").append(r.key).append("';")
	//str.append(fmt.Sprintf(" where __key__ = '%s';", r.key))
	_, err := this.db.Exec(str.toString(), this.values...)
	return err
}

func (this *sqlUpdater) doDelete(r *record) error {
	str := strGet()
	defer strPut(str)
	str.append("delete from ").append(r.table).append(" where __key__ = '").append(r.key).append("';")
	_, err := this.db.Exec(str.toString())
	return err
}

func (this *sqlUpdater) appendDefer(wb *record) {
	//为了防止错误重置writeback标记，需要核对版本号
	wb.ckey.clearWriteBack(wb.writeBackVer)
	recordPut(wb)
}

func (this *sqlUpdater) append(v interface{}) {

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

		wb := v.(*record)

		defer this.appendDefer(wb)

		if this.writeFileAndBreak {
			backupRecord(wb)
			return
		}

		var err error

		for {

			if wb.writeBackFlag == write_back_update {
				err = this.doUpdate(wb)
			} else if wb.writeBackFlag == write_back_insert {
				err = this.doInsert(wb, wb.ckey.meta)
			} else if wb.writeBackFlag == write_back_delete {
				err = this.doDelete(wb)
			} else {
				return
			}

			if nil == err {
				if wb.ctx != nil {
					onSqlWriteBackResp(wb.ctx, errcode.ERR_OK)
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
						onSqlWriteBackResp(wb.ctx, errcode.ERR_SQLERROR)
					}
					Errorln("sqlUpdater exec error:", err)
					return
				}
			}
		}
	}

}

func processWriteBackRecord(now int64) {
	head := pendingWB.Front()
	for ; nil != head; head = pendingWB.Front() {
		wb := head.Value.(*record)
		if !wb.droped && wb.expired > now {
			break
		} else {
			pendingWB.Remove(head)
			if !wb.droped {
				delete(writeBackRecords, wb.uniKey)
				if wb.writeBackFlag != write_back_none {
					//投入执行
					Debugln("pushSQLUpdate", wb.uniKey, wb.writeBackFlag)
					sqlUpdateQueue[StringHash(wb.uniKey)%conf.SqlUpdatePoolSize].Add(wb)
				}
			} else {
				Debugln("processWriteBackRecord record is droped", wb.uniKey)
				recordPut(wb)
			}
		}
	}
}

/*
*    insert + update = insert
*    insert + delete = none
*    insert + insert = 非法
*    update + insert = 非法
*    update + delete = delete
*    update + update = update
*    delete + insert = update
*    delete + update = 非法
*    delete + delte  = 非法
*    none   + insert = insert
*    none   + update = 非法
*    node   + delete = 非法
 */

func addRecord(now int64, ctx *processContext) {

	if ctx.writeBackFlag == write_back_none {
		Errorln("ctx.writeBackFlag == write_back_none")
		return
	}

	uniKey := ctx.getUniKey()
	wb, ok := writeBackRecords[uniKey]
	if !ok {
		wb = recordGet()
		wb.writeBackFlag = ctx.writeBackFlag
		wb.key = ctx.getKey()
		wb.table = ctx.getTable()
		wb.uniKey = uniKey
		wb.ckey = ctx.getCacheKey()
		wb.expired = time.Now().Unix() + conf.WriteBackDelay
		wb.writeBackVer = wb.ckey.writeBackVer
		if wb.writeBackFlag == write_back_insert || wb.writeBackFlag == write_back_update {
			wb.fields = map[string]*proto.Field{}
			for k, v := range ctx.fields {
				wb.fields[k] = v
			}
		}

		if ctx.replyOnDbOk {
			//立即执行
			Debugln("pushSQLUpdate", wb.uniKey, wb.writeBackFlag)
			wb.ctx = ctx
			sqlUpdateQueue[StringHash(wb.uniKey)%conf.SqlUpdatePoolSize].Add(wb)
			return
		} else {
			writeBackRecords[uniKey] = wb
			pendingWB.PushBack(wb)
		}

	} else {
		wb.writeBackVer = wb.ckey.writeBackVer
		//合并状态
		if wb.writeBackFlag == write_back_insert {
			/*
			*    insert + update = insert
			*    insert + delete = none
			*    insert + insert = 非法
			 */
			if ctx.writeBackFlag == write_back_delete {
				wb.fields = nil
				wb.writeBackFlag = write_back_none
			} else {
				for k, v := range ctx.fields {
					wb.fields[k] = v
				}
			}
		} else if wb.writeBackFlag == write_back_update {
			/*
			 *    update + insert = 非法
			 *    update + delete = delete
			 *    update + update = update
			 */
			if ctx.writeBackFlag == write_back_insert {
				//逻辑错误，记录日志
			} else if ctx.writeBackFlag == write_back_delete {
				wb.fields = nil
				wb.writeBackFlag = write_back_delete
			} else {
				for k, v := range ctx.fields {
					wb.fields[k] = v
				}
			}
		} else if wb.writeBackFlag == write_back_delete {
			/*
			*    delete + insert = update
			*    delete + update = 非法
			*    delete + delte  = 非法
			 */
			if ctx.writeBackFlag == write_back_insert {
				wb.fields = map[string]*proto.Field{}
				for k, v := range ctx.fields {
					wb.fields[k] = v
				}
				meta := wb.ckey.meta.fieldMetas
				for k, v := range meta {
					if nil == wb.fields[k] {
						//使用默认值填充
						wb.fields[k] = proto.PackField(k, v.defaultV)
					}
				}
				wb.writeBackFlag = write_back_update
			} else {
				//逻辑错误，记录日志
			}
		} else {
			/*
			*    none   + insert = insert
			*    none   + update = 非法
			*    node   + delete = 非法
			 */
			if ctx.writeBackFlag == write_back_insert {
				wb.fields = map[string]*proto.Field{}
				for k, v := range ctx.fields {
					wb.fields[k] = v
				}
				meta := wb.ckey.meta.fieldMetas
				for k, v := range meta {
					if nil == wb.fields[k] {
						//使用默认值填充
						wb.fields[k] = proto.PackField(k, v.defaultV)
					}
				}
				wb.writeBackFlag = write_back_insert
			} else {
				//逻辑错误，记录日志
			}
		}

		if ctx.replyOnDbOk {
			delete(writeBackRecords, wb.uniKey)
			wb.ctx = ctx
			wb.droped = true
			//立即执行
			Debugln("pushSQLUpdate", wb.uniKey, wb.writeBackFlag)
			sqlUpdateQueue[StringHash(wb.uniKey)%conf.SqlUpdatePoolSize].Add(wb)
			return
		}
	}
}

func writeBackRoutine() {
	for {
		closed, localList := writeBackEventQueue.Get()
		size := len(localList)
		if size > 0 {
			writeBackBarrier_.sub(size)
		}
		now := time.Now().Unix()
		for _, v := range localList {
			switch v.(type) {
			case notifyWB:
				processWriteBackRecord(now + conf.WriteBackDelay + 1)
				break
			case *processContext:
				ctx := v.(*processContext)
				addRecord(now, ctx)
				break
			default:
				processWriteBackRecord(now)
				break
			}
		}

		if conf.WriteBackDelay == 0 || isStop() {
			//延迟为0或服务准备停止,立即执行回写处理
			processWriteBackRecord(now + conf.WriteBackDelay + 1)
		}

		if closed {
			return
		}
	}
}
