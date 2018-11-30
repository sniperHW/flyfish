package flyfish

import (
	"database/sql"
	"fmt"
	"strings"
	"flyfish/conf"
	"time"
	"flyfish/errcode"
)

const queryTemplate = `SELECT %s FROM %s where __key__ in(%s);`

type sqlGet struct {
	table string
	meta  *table_meta	
	keys  []string
	stms  map[string]*cmdStm
}

type sqlLoader struct {
	sqlGets   map[string]*sqlGet
	count     int
	max       int
	db        *sql.DB
}

func newSqlLoader(max int,dbname string,user string,password string) *sqlLoader {
	t := &sqlLoader {
		sqlGets : map[string]*sqlGet{},
		max       : max,
	}
	t.db,_ = pgOpen(dbname,user,password)
	return t
}

func (this *sqlLoader) Reset() {
	this.sqlGets = map[string]*sqlGet{}
	this.count     = 0
}

func (this *sqlLoader) append(v interface{}) {
	stm := v.(*cmdStm)
	s,ok := this.sqlGets[stm.table]
	if !ok {
		s = &sqlGet{
			table : stm.table,
			keys  : []string{},
			stms  : map[string]*cmdStm{},
			meta  : GetMetaByTable(stm.table),
		}
		this.sqlGets[stm.table] = s
	}
	s.keys = append(s.keys,fmt.Sprintf("'%s'",stm.key))
	s.stms[stm.key] = stm
	this.count++

	if this.count >= this.max {
		this.exec()
	}
}


func (this *sqlLoader) onScanError() {
	for _,v := range(this.sqlGets) {
		for _,vv := range(v.stms) {
			onSqlExecError(vv)
		}
	}
}

func (this *sqlLoader) exec() {

	if this.count == 0 {
		return
	}

	defer this.Reset()

	str := ``
	resultSets := []*sqlGet{}
	for _,v := range(this.sqlGets) {
		resultSets = append(resultSets,v)
		str += fmt.Sprintf(queryTemplate,
			strings.Join(v.meta.queryMeta.field_names,","),
			v.table,
			strings.Join(v.keys,","))
	}

	beg := time.Now()

	rows, err := this.db.Query(str)

	if nil != err {
		Errorln("sqlQueryer exec error:",err)
		for _,v := range(this.sqlGets) {
			for _,vv := range(v.stms) {
				onSqlExecError(vv)
			}
		}
		return
	}

	elapse := time.Now().Sub(beg)

	if elapse/time.Millisecond > 500 {
		Infoln("sqlQueryer long exec",elapse,this.count)
	}

	defer rows.Close()

	for _,v := range(resultSets) {

		filed_receiver := v.meta.queryMeta.getReceiver()
		field_convter  := v.meta.queryMeta.field_convter
		field_names    := v.meta.queryMeta.field_names

		for rows.Next() {
			err := rows.Scan(filed_receiver...)
			if err	!= nil {
				Errorln("rows.Scan err",err)
				v.meta.queryMeta.putReceiver(filed_receiver)
				this.onScanError()
				return
			} else {
				key := field_convter[0](filed_receiver[0]).(string)
				stm := v.stms[key]
				if nil != stm {
					//填充返回值
					for i := 1 ; i < len(filed_receiver); i++ {
						name := field_names[i]
						setField := true
						if stm.cmdType == cmdSet {
							if _,ok := stm.fields[name];ok {
								setField = false
							}
						}
						if setField {
							stm.fields[name] = field {
								name  : name,
								value : field_convter[i](filed_receiver[i]),
							}							
						}
					}
					delete(v.stms,key)
					//返回给主循环
					onSqlResp(stm,errcode.ERR_OK)
				}
			}
		}

		for _,vv := range(v.stms) {
			//无结果
			onSqlResp(vv,errcode.ERR_NOTFOUND)
		}
		v.meta.queryMeta.putReceiver(filed_receiver)
		rows.NextResultSet()
	}	
}


func pushSQLLoad(stm *cmdStm) {
	//根据stm.unikey计算hash投递到单独的routine中处理
	hash := StringHash(stm.uniKey)
	sqlLoadQueue[hash%conf.SqlLoadPoolSize].Add(stm)
}


