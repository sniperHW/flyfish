package flyfish

import (
	"container/list"
	"flyfish/conf"
	"fmt"
	"sync"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/sniperHW/kendynet/util"
)

var (
	sql_once            sync.Once
	sqlLoadQueue        []*util.BlockQueue //for get
	sqlUpdateQueue      []*util.BlockQueue //for set/del
	writeBackRecords    map[string]*record
	writeBackEventQueue *util.BlockQueue
	pendingWB           *list.List
	writeBackBarrier_   writeBackBarrier
)

func prepareRecord(ctx *processContext) *record {
	uniKey := ctx.getUniKey()
	wb := recordGet()
	wb.writeBackFlag = ctx.writeBackFlag
	wb.key = ctx.getKey()
	wb.table = ctx.getTable()
	wb.uniKey = uniKey
	wb.ckey = ctx.getCacheKey()

	if wb.writeBackFlag == write_back_insert || wb.writeBackFlag == write_back_update {
		wb.fields = ctx.fields
	}
	return wb
}

type notifyWB struct{}

func closeWriteBack() {
	for _, v := range sqlUpdateQueue {
		v.Close()
	}
}

func notiForceWriteBack() {
	if nil == writeBackEventQueue.AddNoWait(notifyWB{}) {
		writeBackBarrier_.add()
	}
}

func pushSQLWriteBackNoWait(ctx *processContext) {
	ckey := ctx.getCacheKey()
	ckey.setWriteBack()
	if nil == writeBackEventQueue.AddNoWait(ctx) {
		writeBackBarrier_.add()
	}
}

func pushSQLLoad_(q *util.BlockQueue, ctx *processContext, fullReturn ...bool) bool {

	ckey := ctx.getCacheKey()
	if ckey.isWriteBack() {
		/*
		*   如果记录正在等待回写，redis崩溃，导致重新从数据库载入数据，
		*   此时回写尚未完成，如果允许读取将可能载入过期数据
		 */
		//通告回写处理立即执行回写
		notiForceWriteBack()
		/*
		 *  丢弃所有命令，让客户端等待超时
		 */
		ckey.clearCmd()

		return false

	} else {
		err := q.AddNoWait(ctx, fullReturn...)
		if nil == err {
			return true
		} else {
			return false
		}
		/*if noWait {
			q.AddNoWait(ctx)
		} else {
			q.Add(ctx)
		}
		return true*/
	}
}

func pushSQLLoadNoWait(ctx *processContext, fullReturn ...bool) bool {
	q := sqlLoadQueue[StringHash(ctx.getUniKey())%conf.DefConfig.SqlLoadPoolSize]
	return pushSQLLoad_(q, ctx, fullReturn...)
}

/*func pushSQLLoad(ctx *processContext) bool {
	q := sqlLoadQueue[StringHash(ctx.getUniKey())%conf.DefConfig.SqlLoadPoolSize]
	return pushSQLLoad_(q, ctx, false)
}*/

type sqlPipeliner interface {
	append(v interface{})
	exec()
}

func sqlRoutine(queue *util.BlockQueue, pipeliner sqlPipeliner) {
	for {
		closed, localList := queue.Get()
		for _, v := range localList {
			pipeliner.append(v)
		}
		pipeliner.exec()
		if closed {
			switch pipeliner.(type) {
			case *sqlUpdater:
				Infoln("sqlUpdater end")
				writeBackWG.Done()
				break
			default:
				break
			}
			return
		}
	}
}

func SQLInit(host string, port int, dbname string, user string, password string) bool {

	ping := func(q *util.BlockQueue) {
		for {
			//每60秒请求一次ping
			if util.ErrQueueClosed == q.AddNoWait(&processContext{ping: true}) {
				break
			}
			time.Sleep(time.Second * 60)
		}
	}

	sql_once.Do(func() {

		if conf.DefConfig.DBConfig.SqlType == "pgsql" {
			insertPlaceHolder = pgInsertPlaceHolder
			updatePlaceHolder = pgUpdatePlaceHolder
		} else {
			insertPlaceHolder = mysqlInsertPlaceHolder
			updatePlaceHolder = mysqlUpdatePlaceHolder
		}

		pendingWB = list.New()

		writeBackBarrier_.cond = sync.NewCond(&writeBackBarrier_.mtx)

		writeBackRecords = map[string]*record{}
		writeBackEventQueue = util.NewBlockQueueWithName("writeBackEventQueue", conf.DefConfig.WriteBackEventQueueSize)

		sqlLoadQueue = make([]*util.BlockQueue, conf.DefConfig.SqlLoadPoolSize)
		for i := 0; i < conf.DefConfig.SqlLoadPoolSize; i++ {
			name := fmt.Sprintf("sqlLoad:%d", i)
			sqlLoadQueue[i] = util.NewBlockQueueWithName(name, conf.DefConfig.SqlLoadEventQueueSize)

			var db *sqlx.DB
			if conf.DefConfig.DBConfig.SqlType == "pgsql" {
				db, _ = pgOpen(host, port, dbname, user, password)
			} else {
				db, _ = mysqlOpen(host, port, dbname, user, password)
			}
			go sqlRoutine(sqlLoadQueue[i], newSqlLoader(db)) //newSqlLoader(conf.SqlLoadPipeLineSize, host, port, dbname, user, password))
			go ping(sqlLoadQueue[i])
		}

		sqlUpdateQueue = make([]*util.BlockQueue, conf.DefConfig.SqlUpdatePoolSize)
		for i := 0; i < conf.DefConfig.SqlUpdatePoolSize; i++ {
			writeBackWG.Add(1)
			name := fmt.Sprintf("sqlUpdater:%d", i)
			sqlUpdateQueue[i] = util.NewBlockQueueWithName(name, conf.DefConfig.SqlUpdateEventQueueSize)
			//db, _ := pgOpen(host, port, dbname, user, password)
			var db *sqlx.DB
			if conf.DefConfig.DBConfig.SqlType == "pgsql" {
				db, _ = pgOpen(host, port, dbname, user, password)
			} else {
				db, _ = mysqlOpen(host, port, dbname, user, password)
			}
			go sqlRoutine(sqlUpdateQueue[i], newSqlUpdater(name, db)) //newSqlUpdater(name, conf.SqlUpdatePipeLineSize, host, port, dbname, user, password))
			go ping(sqlUpdateQueue[i])
		}

		go writeBackRoutine()

		go func() {
			for {
				time.Sleep(time.Second)
				writeBackEventQueue.Add(struct{}{})
			}
		}()
	})

	return true
}
