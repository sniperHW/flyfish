package flyfish

import (
	"github.com/jmoiron/sqlx"
	"fmt"
	"strings"
	"flyfish/conf"
	"time"
	protocol "flyfish/proto"
)

const updateInsertTemplate = `INSERT INTO %s(%s) VALUES(%s) ON conflict(__key__)  DO UPDATE SET %s;`

const updateTemplate = `UPDATE %s SET %s where __key__ = '%s' ;`

const insertTemplate = `INSERT INTO %s(%s) VALUES(%s);`

const delTemplate = `delete from %s where __key__ in(%s);`

type record struct {
	next           *record
	writeBackFlag  int
	key            string
	table          string
	uniKey         string	
	ckey           *cacheKey
	fields         map[string]*protocol.Field        //所有命令的字段聚合
	expired        int64
}


type delSet struct {
	table string
	keys  []string
}

type sqlUpdater struct {
	delSets   map[string]*delSet
	updates   []string
	count     int
	max       int
	db        *sqlx.DB	
}

func newSqlUpdater(max int,dbname string,user string,password string) *sqlUpdater {
	t := &sqlUpdater {
		delSets : map[string]*delSet{},
		updates : []string{},
		max     : max,
	}
	t.db,_ = pgOpen(dbname,user,password)

	return t
}

func (this *sqlUpdater) Reset() {
	this.delSets = map[string]*delSet{}
	this.updates = []string{}
	this.count   = 0
}


func (this *sqlUpdater) appendSet(wb *record) {

	if wb.writeBackFlag == write_back_insert {
		filedsStatement := "__key__"
		valuesStatement := fmt.Sprintf("'%s'",wb.key)		
		for _,v := range(wb.fields) {
			sqlStr := v.ToSqlStr()
			filedsStatement += ("," + v.GetName())
			valuesStatement += ("," + sqlStr)
		}
		str := fmt.Sprintf(insertTemplate,wb.table,filedsStatement,valuesStatement)
		this.updates = append(this.updates,str)

	} else if wb.writeBackFlag == write_back_update {
		setStatement    := ""
		i := 0
		for _,v := range(wb.fields) {
			sqlStr := v.ToSqlStr()
			if i == 0 {
				i = 1
				setStatement += fmt.Sprintf("%s = %s",v.GetName(),sqlStr)
			} else {
				setStatement += fmt.Sprintf(",%s = %s",v.GetName(),sqlStr)
			}
		}
		str := fmt.Sprintf(updateTemplate,wb.table,setStatement,wb.key)
		this.updates = append(this.updates,str)
	} else {
		Errorln("invaild writeBackFlag",wb.writeBackFlag)
	}	
}

func (this *sqlUpdater) appendDel(wb *record) {
	s,ok := this.delSets[wb.table]
	if !ok {
		s = &delSet{
			table : wb.table,
			keys  : []string{},
		}
		this.delSets[wb.table] = s
	}
	s.keys = append(s.keys,fmt.Sprintf("'%s'",wb.key))
}

func (this *sqlUpdater) append(v interface{}) {
	wb :=  v.(*record) 
	if wb.writeBackFlag == write_back_update || wb.writeBackFlag == write_back_insert {
		this.appendSet(wb)
	} else if wb.writeBackFlag == write_back_delete {
		this.appendDel(wb)
	} else {
		return
	}
	this.count++
	if this.count >= this.max {
		this.exec()
	}
}


func (this *sqlUpdater) exec() {

	if this.count == 0 {
		return
	}

	defer this.Reset()

	str := ``

	beg := time.Now()

	for _,v := range(this.delSets) {
		str += fmt.Sprintf(delTemplate,v.table,strings.Join(v.keys,","))
	}

	str += strings.Join(this.updates,"")

	rows, err := this.db.Query(str)
	
	elapse := time.Now().Sub(beg)

	if elapse/time.Millisecond > 500 {
		Infoln("sqlUpdater long exec",elapse,this.count)
	}

	if nil != err {
		Errorln("sqlUpdater exec error:",err,str)
		fmt.Println("sqlUpdater exec error:",err,str)
	} else {
		rows.Close()
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
		wb := &record{
			writeBackFlag : ctx.writeBackFlag,
			key     : ctx.getKey(),
			table   : ctx.getTable(),
			uniKey  : uniKey,
			ckey    : ctx.getCacheKey(),
		}
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