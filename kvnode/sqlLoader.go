package kvnode

import (
	"github.com/jmoiron/sqlx"
	"github.com/sniperHW/flyfish/conf"
	"github.com/sniperHW/flyfish/dbmeta"
	"github.com/sniperHW/flyfish/errcode"
	"github.com/sniperHW/flyfish/proto"
	"github.com/sniperHW/flyfish/util/str"
	"github.com/sniperHW/kendynet/util"
	"reflect"
	"time"
)

/*
 * 一个要获取的集合
 */
type sqlGet struct {
	table  string
	meta   *dbmeta.TableMeta
	sqlStr *str.Str
	tasks  map[string]asynCmdTaskI
}

type sqlLoader struct {
	sqlGets  map[string]*sqlGet //要获取的结果集
	count    int
	max      int
	db       *sqlx.DB
	lastTime time.Time
	queue    *util.BlockQueue
}

func newSqlLoader(db *sqlx.DB, name string) *sqlLoader {
	config := conf.GetConfig()
	return &sqlLoader{
		sqlGets: map[string]*sqlGet{},
		max:     config.SqlLoadPipeLineSize,
		queue:   util.NewBlockQueueWithName(name, config.SqlLoadQueueSize),
		db:      db,
	}
}

func (this *sqlLoader) Reset() {
	this.sqlGets = map[string]*sqlGet{}
	this.count = 0
}

func (this *sqlLoader) append(v interface{}) {
	switch v.(type) {
	case sqlPing:
		if time.Now().Sub(this.lastTime) > time.Second*5*60 {
			//空闲超过5分钟发送ping
			err := this.db.Ping()
			if nil != err {
				logger.Errorf("ping error %v\n", err)
			}
			this.lastTime = time.Now()
		}
	case asynCmdTaskI:

		task := v.(asynCmdTaskI)
		kv := task.getKV()
		table := kv.table
		key := kv.key
		s, ok := this.sqlGets[table]
		if !ok {
			s = &sqlGet{
				table:  table,
				sqlStr: str.Get(),
				tasks:  map[string]asynCmdTaskI{},
				meta:   kv.getMeta(),
			}
			this.sqlGets[table] = s
		}

		if s.sqlStr.Len() == 0 {
			s.sqlStr.AppendString(s.meta.GetSelectPrefix()).AppendString("'").AppendString(key).AppendString("'")
		} else {
			s.sqlStr.AppendString(",'").AppendString(key).AppendString("'")
		}

		s.tasks[key] = task
		this.count++

		if this.count >= this.max {
			this.exec()
		}

	}
}

func (this *sqlLoader) onSqlError() {
	for _, v := range this.sqlGets {
		for _, vv := range v.tasks {
			vv.onSqlResp(errcode.ERR_SQLERROR)
		}
	}
}

func (this *sqlLoader) exec() {

	if this.count == 0 {
		return
	}

	defer this.Reset()

	this.lastTime = time.Now()

	for _, v := range this.sqlGets {
		v.sqlStr.AppendString(");")
		s := v.sqlStr.ToString()

		beg := time.Now()

		rows, err := this.db.Query(s)

		str.Put(v.sqlStr)

		elapse := time.Now().Sub(beg)

		if elapse/time.Millisecond > 500 {
			logger.Infof("sqlQueryer long exec %v %d\n", elapse, this.count)
		}

		if nil != err {
			logger.Errorf("sqlQueryer exec error:%v %s\n", err, reflect.TypeOf(err).String())
			this.onSqlError()
		} else {

			defer rows.Close()

			queryMeta := v.meta.GetQueryMeta()

			filed_receiver := queryMeta.GetReceivers()
			field_convter := queryMeta.GetFieldConvter()
			field_names := queryMeta.GetFieldNames()

			for rows.Next() {
				err := rows.Scan(filed_receiver...)
				if err != nil {
					logger.Errorf("rows.Scan err:%v\n", err)
					queryMeta.PutReceivers(filed_receiver)
					this.onSqlError()
					return
				} else {
					key := field_convter[0](filed_receiver[0]).(string)
					task := v.tasks[key]
					if nil != task {
						//填充返回值
						for i := 1; i < len(filed_receiver); i++ {
							name := field_names[i]
							task.onLoadField(proto.PackField(name, field_convter[i](filed_receiver[i])))
						}
						delete(v.tasks, key)
						//返回给主循环
						task.onSqlResp(errcode.ERR_OK)
					}
				}
			}

			for _, vv := range v.tasks {
				//无结果
				vv.onSqlResp(errcode.ERR_RECORD_NOTEXIST)
			}
			queryMeta.PutReceivers(filed_receiver)
		}
	}
}

func (this *sqlLoader) run() {
	for {
		closed, localList := this.queue.Get()
		for _, v := range localList {
			this.append(v)
		}
		this.exec()
		if closed {
			return
		}
	}
}
