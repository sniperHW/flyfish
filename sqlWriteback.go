package flyfish

import (
	"github.com/jmoiron/sqlx"
	"fmt"
	//"strings"
	"flyfish/conf"
	"time"
	protocol "flyfish/proto"
	"sync"
)

type record struct {
	writeBackFlag  int
	key            string
	table          string
	uniKey         string	
	ckey           *cacheKey
	fields         map[string]*protocol.Field        //所有命令的字段聚合
	expired        int64
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
		for _,name := range(this.ckey.meta.insertFieldOrder) {
			s.append(",").append(this.fields[name].ToSqlStr())
		}
		s.append(")")
	} else {
		s.append(",('").append(this.key).append("',").append(this.fields["__version__"].ToSqlStr())
		for _,name := range(this.ckey.meta.insertFieldOrder) {
			s.append(",").append(this.fields[name].ToSqlStr())
		}
		s.append(")")
	}

	return s.len - oldLen
}

type sqlUpdater struct {
	delSets          map[string]*str
	insertSets       map[string]*str
	updates          *str

	updateStrSize  int
	count          int
	max            int
	db             *sqlx.DB	
}

func newSqlUpdater(max int,host string, port int,dbname string,user string,password string) *sqlUpdater {
	t := &sqlUpdater {
		max     : max,
	}
	t.db,_ = pgOpen(host,port,dbname,user,password)

	return t
}

func (this *sqlUpdater) Reset() {
	
	for _,v := range(this.delSets) {
		strPut(v)
	}
	for _,v := range(this.insertSets) {
		strPut(v)
	}

	if nil != this.updates {
		strPut(this.updates)
	}

	this.delSets = nil
	this.insertSets = nil
	this.updates = nil

	this.count   = 0
	this.updateStrSize = 0
}

func (this *sqlUpdater) appendDel(wb *record) {
	
	if nil == this.delSets {
		this.delSets = map[string]*str{}
	}

	s,ok := this.delSets[wb.table]
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

	s,ok := this.insertSets[wb.table]
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
	for _,v := range(wb.fields) {
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
	wb :=  v.(*record) 
	
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
	this.count++
	if this.count >= this.max || this.updateStrSize >= conf.MaxUpdateStringSize {
		this.exec()
	}
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
		for _,v := range(this.delSets) {
			s.append(v.toString()).append(");")
		}
	}

	if nil != this.insertSets {
		for _,v := range(this.insertSets) {
			s.append(v.toString()).append(";")
		}		
	}

	if nil != this.updates {
		s.append(this.updates.toString())
	}

	_ , err := this.db.Exec(s.toString())

	//Errorln(s.toString())

	elapse := time.Now().Sub(beg)

	if elapse/time.Millisecond > 500 {
		Infoln("sqlUpdater long exec",elapse,this.count)
	}

	if nil != err {
		Errorln("sqlUpdater exec error:",err,s.toString())
		fmt.Println("sqlUpdater exec error:",err,s.toString())
	}

	strPut(s)

}


func processWriteBackRecord(now int64) {
	head := pendingWB.Front()
	for ; nil != head; head = pendingWB.Front() {
		wb := head.Value.(*record)
		if wb.expired > now {
			break
		} else {
			pendingWB.Remove(head)
			delete(writeBackRecords,wb.uniKey)
			if wb.writeBackFlag != write_back_none {
				//投入执行
				Debugln("pushSQLUpdate",wb.uniKey)
				hash := StringHash(wb.uniKey)
				sqlUpdateQueue[hash%conf.SqlUpdatePoolSize].Add(wb)
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

func addRecord(now int64,ctx *processContext) {
	
	if ctx.writeBackFlag == write_back_none {
		panic("ctx.writeBackFlag == write_back_none")
		return
	}

	uniKey := ctx.getUniKey()
	wb,ok := writeBackRecords[uniKey]
	if !ok {
		/*wb := &record{
			writeBackFlag : ctx.writeBackFlag,
			key     : ctx.getKey(),
			table   : ctx.getTable(),
			uniKey  : uniKey,
			ckey    : ctx.getCacheKey(),
		}*/

		wb = recordGet()
		wb.writeBackFlag = ctx.writeBackFlag
		wb.key = ctx.getKey()
		wb.table = ctx.getTable()
		wb.uniKey = uniKey
		wb.ckey = ctx.getCacheKey()

		writeBackRecords[uniKey] = wb
		if wb.writeBackFlag == write_back_insert || wb.writeBackFlag == write_back_update {
			wb.fields = map[string]*protocol.Field{}
			for k,v := range(ctx.fields) {
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
				for k,v := range(ctx.fields) {
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
				for k,v := range(ctx.fields) {
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
				wb.fields = map[string]*protocol.Field{}
				for k,v := range(ctx.fields) {
					wb.fields[k] = v
				}
				meta := wb.ckey.meta.fieldMetas
				for k,v := range(meta) {
					if nil == wb.fields[k] {
						//使用默认值填充
						wb.fields[k] = protocol.PackField(k,v.defaultV)
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
				wb.fields = map[string]*protocol.Field{}
				for k,v := range(ctx.fields) {
					wb.fields[k] = v
				}
				meta := wb.ckey.meta.fieldMetas
				for k,v := range(meta) {
					if nil == wb.fields[k] {
						//使用默认值填充
						wb.fields[k] = protocol.PackField(k,v.defaultV)
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
		now := time.Now().Unix()
		for _,v := range(localList) {
			switch v.(type) {
			case *processContext:			
				ctx := v.(*processContext)
				addRecord(now,ctx)
				break
			default:
				processWriteBackRecord(now)
				break
			}
		}

		if closed {
			return
		}
	}	
}