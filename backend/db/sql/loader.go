package sql

import (
	"github.com/jmoiron/sqlx"
	"github.com/sniperHW/flyfish/backend/db"
	"github.com/sniperHW/flyfish/pkg/buffer"
	"github.com/sniperHW/flyfish/pkg/queue"
	"github.com/sniperHW/flyfish/proto"
	"reflect"
	"sync"
	"time"
	"unsafe"
)

type ping int

const sqlping ping = ping(1)

/*
 *  从队列读取连续的请求,根据请求的table把请求分组成不同的select语句
 */

//一个查询组产生select * from table where key in ()语句
type query struct {
	table string
	meta  *TableMeta
	tasks map[string]db.DBLoadTask
	buff  *buffer.Buffer
}

func (q *query) onResult(err error, version int64, fields map[string]*proto.Field) {
	for _, v := range q.tasks {
		v.OnResult(err, version, fields)
	}
}

type loader struct {
	queryGroup map[string]*query //要获取的结果集
	count      int
	max        int
	dbc        *sqlx.DB
	lastTime   time.Time
	que        *queue.ArrayQueue
	stoponce   sync.Once
}

func (this *loader) IssueLoadTask(t db.DBLoadTask) error {
	return this.que.Append(t)
}

func (this *loader) Stop() {
	this.stoponce.Do(func() {
		this.que.Close()
	})
}

func (this *loader) Start() {
	go func() {
		for {
			time.Sleep(time.Second * 60)
			if nil != this.que.ForceAppend(sqlping) {
				return
			}
		}
	}()

	go func() {
		localList := make([]interface{}, 0, 200)
		closed := false
		for {

			localList, closed = this.que.Pop(localList)
			size := len(localList)
			if closed && size == 0 {
				break
			}

			for i, v := range localList {
				this.append(v)
				localList[i] = nil
			}

			this.exec()
		}
	}()
}

func (this *loader) append(v interface{}) {
	switch v.(type) {
	case ping:
		if time.Now().Sub(this.lastTime) > time.Second*5*60 {
			//空闲超过5分钟发送ping
			err := this.dbc.Ping()
			if nil != err {
				GetSugar().Errorf("ping error %v\n", err)
			}
			this.lastTime = time.Now()
		}
	case db.DBLoadTask:
		task := v.(db.DBLoadTask)
		table := task.GetTable()
		key := task.GetKey()

		q, ok := this.queryGroup[table]
		if !ok {
			q = &query{
				table: table,
				buff:  buffer.Get(),
				tasks: map[string]db.DBLoadTask{},
				meta:  task.GetTableMeta().(*TableMeta),
			}
			this.queryGroup[table] = q
			q.buff.AppendString(q.meta.GetSelectPrefix()).AppendString("'").AppendString(key).AppendString("'")
		} else {
			q.buff.AppendString(",'").AppendString(key).AppendString("'")
		}

		q.tasks[key] = task
		this.count++

		if this.count >= this.max {
			this.exec()
		}
	}
}

func (this *loader) reset() {
	this.queryGroup = map[string]*query{}
	this.count = 0
}

func (this *loader) exec() {

	if this.count == 0 {
		return
	}

	defer this.reset()

	this.lastTime = time.Now()

	for _, v := range this.queryGroup {
		v.buff.AppendString(");")
		b := v.buff.Bytes()
		statement := *(*string)(unsafe.Pointer(&b))
		beg := time.Now()
		rows, err := this.dbc.Query(statement)
		v.buff.Free()

		elapse := time.Now().Sub(beg)

		if elapse/time.Millisecond > 500 {
			GetSugar().Infof("sqlQueryer long exec elapse:%v count:%d", elapse, this.count)
		}

		if nil != err {
			GetSugar().Errorf("sqlQueryer exec error:%v %s", err, reflect.TypeOf(err).String())
			v.onResult(db.ERR_DbError, 0, nil)
		} else {

			defer rows.Close()

			queryMeta := v.meta.GetQueryMeta()

			filed_receiver := queryMeta.GetReceiver()
			field_convter := queryMeta.GetFieldConvter()
			field_names := queryMeta.GetFieldNames()

			errCode := db.ERR_RecordNotExist

			for rows.Next() {
				err := rows.Scan(filed_receiver...)
				if err != nil {
					GetSugar().Errorf("rows.Scan err:%v", err)
					errCode = db.ERR_DbError
					break
				} else {

					key := field_convter[0](filed_receiver[0]).(string)
					task := v.tasks[key]
					if nil != task {
						//填充返回值
						version := field_convter[1](filed_receiver[1]).(int64)
						fields := map[string]*proto.Field{}

						for i := 2; i < len(filed_receiver); i++ {
							name := field_names[i]
							fields[name] = proto.PackField(name, field_convter[i](filed_receiver[i]))
						}
						delete(v.tasks, key)
						//返回给主循环
						task.OnResult(nil, version, fields)
					}
				}
			}

			for _, vv := range v.tasks {
				vv.OnResult(errCode, 0, nil)
			}
		}
	}
}

func NewLoader(dbc *sqlx.DB, maxbatchSize int, quesize int) *loader {
	return &loader{
		queryGroup: map[string]*query{},
		max:        maxbatchSize,
		que:        queue.NewArrayQueue(quesize),
		dbc:        dbc,
	}
}
