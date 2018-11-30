package flyfish

import (
	"database/sql"
	"fmt"
	"strings"
	"flyfish/conf"
	"time"
)

const updateInsertTemplate = `INSERT INTO %s(%s) VALUES(%s) ON conflict(__key__)  DO UPDATE SET %s;`

const updateTemplate = `UPDATE %s SET %s where __key__ = '%s' ;`

const insertTemplate = `INSERT INTO %s(%s) VALUES(%s);`

const delTemplate = `delete from %s where __key__ in(%s);`

type writeBackQueue struct {
	head       *writeBack
	tail       *writeBack      	
}

func (this *writeBackQueue) Push(w *writeBack) {
	if nil != this.tail {
		this.tail.next = w
		this.tail = w
	} else {
		this.head = w
		this.tail = w
	}
}

func (this *writeBackQueue) Pop() *writeBack {
	if nil == this.head {
		return nil
	} else {
		head := this.head
		this.head = head.next
		if this.head == nil {
			this.tail = nil
		}
		return head
	}
}

func (this *writeBackQueue) Head() *writeBack {
	return this.head
}


type writeBack struct {
	next           *writeBack
	writeBackFlag  int
	key            string
	table          string
	uniKey         string	
	ckey           *cacheKey
	fields         map[string]field        //所有命令的字段聚合
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
	db        *sql.DB	
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


func (this *sqlUpdater) appendSet(wb *writeBack) {

	if wb.writeBackFlag == write_back_insert {
		filedsStatement := "__key__"
		valuesStatement := fmt.Sprintf("'%s'",wb.key)		
		for _,v := range(wb.fields) {
			sqlStr := v.ToSqlStr()
			filedsStatement += ("," + v.name)
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
				setStatement += fmt.Sprintf("%s = %s",v.name,sqlStr)
			} else {
				setStatement += fmt.Sprintf(",%s = %s",v.name,sqlStr)
			}
		}
		str := fmt.Sprintf(updateTemplate,wb.table,setStatement,wb.key)
		this.updates = append(this.updates,str)
	} else {
		Errorln("invaild writeBackFlag",wb.writeBackFlag)
	}
	
/*	filedsStatement := "__key__"
	valuesStatement := fmt.Sprintf("'%s'",stm.key)
	setStatement    := ""
	
	i := 0
	for _,v := range(stm.fields) {
		sqlStr := v.ToSqlStr()
		filedsStatement += ("," + v.name)
		valuesStatement += ("," + sqlStr)
		if i == 0 {
			i = 1
			setStatement += fmt.Sprintf("%s = %s",v.name,sqlStr)
		} else {
			setStatement += fmt.Sprintf(",%s = %s",v.name,sqlStr)
		}
	}

	//const updateInsertTemplate = `INSERT INTO %s(%s) VALUES(%s) ON conflict(__key__)  DO UPDATE SET %s;`
	this.updates = append(this.updates,fmt.Sprintf(updateInsertTemplate,stm.table,filedsStatement,valuesStatement,setStatement))
*/	
}

func (this *sqlUpdater) appendDel(wb *writeBack) {
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
	wb :=  v.(*writeBack) 
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

func pushSQLWriteBack(stm *cmdStm) {
	if conf.WriteBackDelay > 0 {
		//延迟回写
		writeBackEventQueue.Add(stm)
	} else {
		//直接回写
		wb := &writeBack{
			writeBackFlag : stm.writeBackFlag,
			key : stm.key,
			table : stm.table,
			uniKey : stm.uniKey,
			ckey : stm.ckey,
			fields  : map[string]field{}, 
		}
		if wb.writeBackFlag == write_back_insert || wb.writeBackFlag == write_back_update {
			for k,v := range(stm.fields) {
				wb.fields[k] = field {
					name  : k,
					value : v.value,
				}
			}
		}
		hash := StringHash(stm.uniKey)
		sqlUpdateQueue[hash%conf.SqlUpdatePoolSize].Add(wb)		
	}
}

func processWriteBackQueue(now int64) {
	wb := writeBackQueue_.Head()
	for ; nil != wb; wb = writeBackQueue_.Head() {
		if wb.expired > now {
			break
		} else {
			writeBackQueue_.Pop()
			delete(writeBackKeys,wb.uniKey)
			//投入执行
			Debugln("pushSQLUpdate",wb.uniKey)
			hash := StringHash(wb.uniKey)
			sqlUpdateQueue[hash%conf.SqlUpdatePoolSize].Add(wb)
		}
	}
}

func addWriteBack(now int64,stm *cmdStm) {
	wb := writeBackKeys[stm.uniKey]
	if nil == wb {
		wb = &writeBack{
			writeBackFlag : stm.writeBackFlag,
			key : stm.key,
			table : stm.table,
			uniKey : stm.uniKey,
			ckey : stm.ckey,
			expired : now + conf.WriteBackDelay,
			fields  : map[string]field{}, 
		}
		writeBackKeys[stm.uniKey] = wb
		writeBackQueue_.Push(wb)
	}
	if wb.writeBackFlag == write_back_insert || wb.writeBackFlag == write_back_update {
		//合并fields
		for k,v := range(stm.fields) {
			wb.fields[k] = field {
				name  : k,
				value : v.value,
			}
		}
	} else {
		if len(wb.fields) > 0 {
			wb.fields = map[string]field{}
		}
	}
	processWriteBackQueue(now)
}

func writeBackRoutine() {
	for {
		closed, localList := writeBackEventQueue.Get()
		now := time.Now().Unix()
		for _,v := range(localList) {
			switch v.(type) {
			case *cmdStm:			
				stm := v.(*cmdStm)
				addWriteBack(now,stm)
				break
			default:
				processWriteBackQueue(now)
				break
			}
		}

		if closed {
			return
		}
	}	
}