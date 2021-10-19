package sql

import (
	"container/list"
	"github.com/jmoiron/sqlx"
	"github.com/sniperHW/flyfish/db"
	"github.com/sniperHW/flyfish/pkg/buffer"
	"github.com/sniperHW/flyfish/pkg/queue"
	"github.com/sniperHW/flyfish/proto"
	"reflect"
	"sync"
	"sync/atomic"
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
	tasks map[string]*list.List
}

func (q *query) onResult(err error, version int64, fields map[string]*proto.Field) {
	for _, v := range q.tasks {
		for t := v.Front(); nil != t; t = v.Front() {
			task := v.Remove(t).(db.DBLoadTask)
			task.OnResult(err, version, fields)
		}
	}
}

type loader struct {
	queryGroup map[string]*query //要获取的结果集
	max        int
	dbc        *sqlx.DB
	lastTime   time.Time
	que        *queue.ArrayQueue
	stoponce   int32
	startOnce  sync.Once
}

func (this *loader) IssueLoadTask(t db.DBLoadTask) error {
	return this.que.Append(t)
}

func (this *loader) Stop() {
	if atomic.CompareAndSwapInt32(&this.stoponce, 0, 1) {
		this.que.Close()
	}
}

func (this *loader) Start() {
	this.startOnce.Do(func() {

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
	})
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
				tasks: map[string]*list.List{},
				meta:  task.GetTableMeta().(*TableMeta),
			}
			this.queryGroup[table] = q
		}

		l, ok := q.tasks[key]
		if !ok {
			l = list.New()
			q.tasks[key] = l
		}

		l.PushBack(task)

		if len(q.tasks) >= this.max {
			this.exec()
		}
	}
}

func (this *loader) reset() {
	this.queryGroup = map[string]*query{}
}

func (this *loader) exec() {
	if len(this.queryGroup) == 0 {
		return
	}

	defer this.reset()

	this.lastTime = time.Now()

	for _, v := range this.queryGroup {
		buff := buffer.Get()
		first := true
		for kk, _ := range v.tasks {
			if first {
				first = false
				buff.AppendString(v.meta.GetSelectPrefix()).AppendString("'").AppendString(kk).AppendString("'")
			} else {
				buff.AppendString(",'").AppendString(kk).AppendString("'")
			}
		}

		buff.AppendString(");")
		b := buff.Bytes()

		statement := *(*string)(unsafe.Pointer(&b))
		beg := time.Now()
		rows, err := this.dbc.Query(statement)
		buff.Free()

		elapse := time.Now().Sub(beg)

		if elapse/time.Millisecond > 500 {
			GetSugar().Infof("sqlQueryer long exec elapse:%v count:%d", elapse, len(v.tasks))
		}

		if nil != err {
			GetSugar().Errorf("sqlQueryer exec error:%v %s", err, reflect.TypeOf(err).String())
			v.onResult(db.ERR_DbError, 0, nil)
		} else {

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
					tasks, ok := v.tasks[key]
					if ok {
						//填充返回值
						version := field_convter[1](filed_receiver[1]).(int64)
						fields := map[string]*proto.Field{}

						for i := 2; i < len(filed_receiver); i++ {
							name := field_names[i]
							fields[name] = proto.PackField(name, field_convter[i](filed_receiver[i]))
						}

						delete(v.tasks, key)

						for t := tasks.Front(); nil != t; t = tasks.Front() {
							task := tasks.Remove(t).(db.DBLoadTask)
							task.OnResult(nil, version, fields)
						}
					}
				}
			}

			rows.Close()

			v.onResult(errCode, 0, nil)
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