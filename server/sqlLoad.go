package server

import (
	"github.com/jmoiron/sqlx"
	"github.com/sniperHW/flyfish/conf"
	"github.com/sniperHW/flyfish/errcode"
	"github.com/sniperHW/flyfish/proto"
	"github.com/sniperHW/kendynet/util"
	"reflect"
	"time"
)

/*
 * 一个要获取的集合
 */
type sqlGet struct {
	table  string
	meta   *table_meta
	sqlStr *str
	ctxs   map[string]*cmdContext
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
	ctx := v.(*cmdContext)
	if ctx.ping {
		if time.Now().Sub(this.lastTime) > time.Second*5*60 {
			//空闲超过5分钟发送ping
			err := this.db.Ping()
			if nil != err {
				Errorln("ping error", err)
			}
			this.lastTime = time.Now()
		}
	} else {
		table := ctx.getTable()
		key := ctx.getKey()
		s, ok := this.sqlGets[table]
		if !ok {
			s = &sqlGet{
				table:  table,
				sqlStr: strGet(),
				ctxs:   map[string]*cmdContext{},
				meta:   ctx.getCacheKey().getMeta(),
			}
			this.sqlGets[table] = s
		}

		if s.sqlStr.len == 0 {
			s.sqlStr.append(s.meta.selectPrefix).append("'").append(key).append("'")
		} else {
			s.sqlStr.append(",'").append(key).append("'")
		}

		s.ctxs[key] = ctx
		this.count++

		if this.count >= this.max {
			this.exec()
		}
	}
}

func (this *sqlLoader) onScanError() {
	for _, v := range this.sqlGets {
		for _, vv := range v.ctxs {
			onSqlExecError(vv)
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
		v.sqlStr.append(");")
		str := v.sqlStr.toString()

		beg := time.Now()

		rows, err := this.db.Query(str)

		strPut(v.sqlStr)

		elapse := time.Now().Sub(beg)

		if elapse/time.Millisecond > 500 {
			Infoln("sqlQueryer long exec", elapse, this.count)
		}

		if nil != err {
			Errorln("sqlQueryer exec error:", err, reflect.TypeOf(err).String())
			for _, vv := range v.ctxs {
				onSqlExecError(vv)
			}
		} else {

			defer rows.Close()

			filed_receiver := v.meta.queryMeta.getReceiver()
			field_convter := v.meta.queryMeta.field_convter
			field_names := v.meta.queryMeta.field_names

			for rows.Next() {
				err := rows.Scan(filed_receiver...)
				if err != nil {
					Errorln("rows.Scan err", err)
					v.meta.queryMeta.putReceiver(filed_receiver)
					this.onScanError()
					return
				} else {
					key := field_convter[0](filed_receiver[0]).(string)
					ctx := v.ctxs[key]
					if nil != ctx {
						//填充返回值
						for i := 1; i < len(filed_receiver); i++ {
							name := field_names[i]
							ctx.fields[name] = proto.PackField(name, field_convter[i](filed_receiver[i]))
						}
						delete(v.ctxs, key)
						//返回给主循环
						onSqlResp(ctx, errcode.ERR_OK)
					}
				}
			}

			for _, vv := range v.ctxs {
				//无结果
				onSqlResp(vv, errcode.ERR_NOTFOUND)
			}
			v.meta.queryMeta.putReceiver(filed_receiver)
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