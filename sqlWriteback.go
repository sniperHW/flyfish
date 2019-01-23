package flyfish

import (
	"database/sql/driver"
	"flyfish/conf"
	"flyfish/proto"
	"fmt"
	"github.com/jmoiron/sqlx"
	//"github.com/sniperHW/kendynet"
	"net"
	"os"
	"sync"
	"time"
)

type writeBackBarrior struct {
	counter int
	waited  int
	mtx     sync.Mutex
	cond    *sync.Cond
}

func (this *writeBackBarrior) add() {
	this.mtx.Lock()
	this.counter++
	this.mtx.Unlock()
}

func (this *writeBackBarrior) sub(c int) {
	this.mtx.Lock()
	this.counter = this.counter - c
	if this.counter < conf.WriteBackEventQueueSize && this.waited > 0 {
		this.mtx.Unlock()
		this.cond.Broadcast()
	} else {
		this.mtx.Unlock()
	}
}

func (this *writeBackBarrior) wait() {
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
}

var recordPool = sync.Pool{
	New: func() interface{} {
		return &record{}
	},
}

func recordGet() *record {
	r := recordPool.Get().(*record)
	r.writeBackFlag = write_back_none
	r.ckey = nil
	r.fields = nil
	return r
}

func recordPut(r *record) {
	recordPool.Put(r)
}

/*
func (this *record) appendDel(s *str) int {

	oldLen := s.len

	if s.len == 0 {
		s.append("delete from ").append(this.table).append(" where __key__ in ('").append(this.key).append("'")
	} else {
		s.append(",'").append(this.key).append("'")
	}

	return s.len - oldLen

}

func (this *record) appendInsert(s *str) int {

	oldLen := s.len

	if s.len == 0 {
		s.append(this.ckey.meta.insertPrefix).append("('").append(this.key).append("',").append(this.fields["__version__"].ToSqlStr())
		for _, name := range this.ckey.meta.insertFieldOrder {
			s.append(",").append(this.fields[name].ToSqlStr())
		}
		s.append(")")
	} else {
		s.append(",('").append(this.key).append("',").append(this.fields["__version__"].ToSqlStr())
		for _, name := range this.ckey.meta.insertFieldOrder {
			s.append(",").append(this.fields[name].ToSqlStr())
		}
		s.append(")")
	}

	return s.len - oldLen
}

type sqlUpdater struct {
	delSets    map[string]*str
	insertSets map[string]*str
	updates    *str
	keys       []*cacheKey

	updateStrSize     int
	count             int
	max               int
	db                *sqlx.DB
	name              string
	file              *os.File
	writeFileAndBreak bool
}

func newSqlUpdater(name string, max int, host string, port int, dbname string, user string, password string) *sqlUpdater {
	t := &sqlUpdater{
		max:  max,
		keys: []*cacheKey{},
		name: name,
	}
	t.db, _ = pgOpen(host, port, dbname, user, password)

	return t
}

func (this *sqlUpdater) Reset() {

	for _, v := range this.delSets {
		strPut(v)
	}
	for _, v := range this.insertSets {
		strPut(v)
	}

	if nil != this.updates {
		strPut(this.updates)
	}

	this.delSets = nil
	this.insertSets = nil
	this.updates = nil
	this.keys = []*cacheKey{}

	this.count = 0
	this.updateStrSize = 0
}

func (this *sqlUpdater) appendDel(wb *record) {

	if nil == this.delSets {
		this.delSets = map[string]*str{}
	}

	s, ok := this.delSets[wb.table]
	if !ok {
		s = strGet()
		this.delSets[wb.table] = s
	}
	this.updateStrSize += wb.appendDel(s)
}

func (this *sqlUpdater) appendInsert(wb *record) {

	if nil == this.insertSets {
		this.insertSets = map[string]*str{}
	}

	s, ok := this.insertSets[wb.table]
	if !ok {
		s = strGet()
		this.insertSets[wb.table] = s
	}
	this.updateStrSize += wb.appendInsert(s)
}

func (this *sqlUpdater) appendUpdate(wb *record) {

	if nil == this.updates {
		this.updates = strGet()
	}

	//const updateTemplate = `UPDATE %s SET %s where __key__ = '%s' ;`
	oldLen := this.updates.len
	this.updates.append("UPDATE ").append(wb.table).append(" SET ")
	i := 0
	for _, v := range wb.fields {
		sqlStr := v.ToSqlStr()
		if i == 0 {
			i = 1
			this.updates.append(v.GetName()).append(" = ").append(sqlStr)
		} else {
			this.updates.append(",").append(v.GetName()).append(" = ").append(sqlStr)
		}
	}
	this.updates.append(" where __key__ = '").append(wb.key).append("';")
	this.updateStrSize += (this.updates.len - oldLen)
}

func (this *sqlUpdater) append(v interface{}) {
	wb := v.(*record)

	defer recordPut(wb)

	if wb.writeBackFlag == write_back_update {
		this.appendUpdate(wb)
	} else if wb.writeBackFlag == write_back_insert {
		this.appendInsert(wb)
	} else if wb.writeBackFlag == write_back_delete {
		this.appendDel(wb)
	} else {
		return
	}
	this.keys = append(this.keys, wb.ckey)
	this.count++
	if this.count >= this.max || this.updateStrSize >= conf.MaxUpdateStringSize {
		this.exec()
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

func (this *sqlUpdater) writeFile(s string) {
	if nil == this.file {

		out_path := fmt.Sprintf("%s/%s_%d.bak", conf.BackDir, this.name, time.Now().Unix())

		f, err := os.OpenFile(out_path, os.O_RDWR, os.ModePerm)
		if err != nil {
			if os.IsNotExist(err) {
				f, err = os.Create(out_path)
				if err != nil {
					Errorf("create %s failed:%s", out_path, err.Error())
					return
				}
			} else {
				Errorf("open %s failed:%s", out_path, err.Error())
				return
			}
		}
		this.file = f
	}

	b := kendynet.NewByteBuffer()
	b.AppendByte(0)
	b.AppendString(s)

	this.file.Write(b.Bytes())
	this.file.Sync()
}

func (this *sqlUpdater) exec() {

	if this.count == 0 {
		return
	}

	defer this.Reset()

	//str := ``

	beg := time.Now()

	s := strGet()

	if nil != this.delSets {
		for _, v := range this.delSets {
			s.append(v.toString()).append(");")
		}
	}

	if nil != this.insertSets {
		for _, v := range this.insertSets {
			s.append(v.toString()).append(";")
		}
	}

	if nil != this.updates {
		s.append(this.updates.toString())
	}

	if this.writeFileAndBreak {
		this.writeFile(s.toString())
		strPut(s)
		for _, v := range this.keys {
			v.clearWriteBack()
		}
		return
	}

	for {

		_, err := this.db.Exec(s.toString())

		elapse := time.Now().Sub(beg)

		if elapse/time.Millisecond > 500 {
			Infoln("sqlUpdater long exec", elapse, this.count)
		}

		if nil != err {
			if isRetryError(err) {
				Errorln("sqlUpdater exec error:", err)
				if isStop() {
					this.writeFileAndBreak = true
					this.writeFile(s.toString())
					strPut(s)
					for _, v := range this.keys {
						v.clearWriteBack()
					}
					break
				}
				//休眠一秒重试
				time.Sleep(time.Second)
			} else {
				Errorln("sqlUpdater exec error:", err, s.toString())
				strPut(s)
				break
			}
		} else {
			strPut(s)
			for _, v := range this.keys {
				v.clearWriteBack()
			}
		}
		break
	}

}*/

type sqlUpdater struct {
	db                *sqlx.DB
	name              string
	file              *os.File
	values            []interface{}
	writeFileAndBreak bool
}

func newSqlUpdater(name string, max int, host string, port int, dbname string, user string, password string) *sqlUpdater {
	t := &sqlUpdater{
		name:   name,
		values: []interface{}{},
	}
	t.db, _ = pgOpen(host, port, dbname, user, password)

	return t
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

func (this *sqlUpdater) doInsert(r *record) error {
	str := strGet()
	defer func() {
		this.resetValues()
		strPut(str)
	}()

	str.append(r.ckey.meta.insertPrefix).append("($1,$2")
	this.values = append(this.values, r.key, r.fields["__version__"].GetValue())
	c := 2
	for _, name := range r.ckey.meta.insertFieldOrder {
		c++
		str.append(fmt.Sprintf(",$%d", c))
		this.values = append(this.values, r.fields[name].GetValue())
	}
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
			str.append(fmt.Sprintf(" %s = $%d", v.GetName(), i))
		} else {
			str.append(fmt.Sprintf(",%s = $%d", v.GetName(), i))
		}
	}
	str.append(fmt.Sprintf(" where __key__ = '%s';", r.table))
	/*defer this.resetValues()
	str := fmt.Sprintf("update %s set ", r.table)
	i := 0
	for _, v := range r.fields {
		this.values = append(this.values, v.GetValue())
		i++
		if i == 1 {
			str += fmt.Sprintf(" %s = $%d", v.GetName(), i)
		} else {
			str += fmt.Sprintf(",%s = $%d", v.GetName(), i)
		}
	}
	str += fmt.Sprintf(" where __key__ = '%s';", r.table)
	*/
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

func (this *sqlUpdater) append(v interface{}) {
	wb := v.(*record)

	defer recordPut(wb)

	var err error

	for {

		if wb.writeBackFlag == write_back_update {
			err = this.doUpdate(wb)
		} else if wb.writeBackFlag == write_back_insert {
			err = this.doInsert(wb)
		} else if wb.writeBackFlag == write_back_delete {
			err = this.doDelete(wb)
		} else {
			return
		}

		if nil == err {
			wb.ckey.clearWriteBack()
			return
		} else {
			if isRetryError(err) {
				Errorln("sqlUpdater exec error:", err)
				/*if isStop() {
					this.writeFileAndBreak = true
					this.writeFile(s.toString())
					for _, v := range this.keys {
						v.clearWriteBack()
					}
					break
				}*/
				//休眠一秒重试
				time.Sleep(time.Second)
			} else {
				Errorln("sqlUpdater exec error:", err)
				return
			}
		}
	}

}

func processWriteBackRecord(now int64) {
	head := pendingWB.Front()
	for ; nil != head; head = pendingWB.Front() {
		wb := head.Value.(*record)
		if wb.expired > now {
			break
		} else {
			pendingWB.Remove(head)
			delete(writeBackRecords, wb.uniKey)
			if wb.writeBackFlag != write_back_none {
				//投入执行
				Debugln("pushSQLUpdate", wb.uniKey)
				sqlUpdateQueue[StringHash(wb.uniKey)%conf.SqlUpdatePoolSize].Add(wb)
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
		panic("ctx.writeBackFlag == write_back_none")
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

		writeBackRecords[uniKey] = wb
		if wb.writeBackFlag == write_back_insert || wb.writeBackFlag == write_back_update {
			wb.fields = map[string]*proto.Field{}
			for k, v := range ctx.fields {
				wb.fields[k] = v
			}
		}
		pendingWB.PushBack(wb)
	} else {
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
	}
}

func writeBackRoutine() {
	for {
		closed, localList := writeBackEventQueue.Get()
		size := len(localList)
		if size > 0 {
			writeBackBarrior_.sub(size)
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
