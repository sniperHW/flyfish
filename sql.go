package flyfish

import (
	"container/list"
	"flyfish/conf"
	"fmt"
	"sync"
	"time"

	"github.com/sniperHW/kendynet/util"
)

var (
	sql_once            sync.Once
	sqlLoadQueue        *util.BlockQueue   //for get
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

func pushSQLLoad_(ctx *processContext, noWait bool) bool {
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
		if noWait {
			sqlLoadQueue.AddNoWait(ctx)
		} else {
			sqlLoadQueue.Add(ctx)
		}
		return true
	}
}

func pushSQLLoadNoWait(ctx *processContext) bool {
	return pushSQLLoad_(ctx, true)
}

func pushSQLLoad(ctx *processContext) bool {
	return pushSQLLoad_(ctx, false)
}

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
	sql_once.Do(func() {

		if conf.SqlType == "pgsql" {
			insertPlaceHolder = pgInsertPlaceHolder
			updatePlaceHolder = pgUpdatePlaceHolder
		} else {
			insertPlaceHolder = mysqlInsertPlaceHolder
			updatePlaceHolder = mysqlUpdatePlaceHolder
		}

		pendingWB = list.New()

		writeBackBarrier_.cond = sync.NewCond(&writeBackBarrier_.mtx)

		writeBackRecords = map[string]*record{}
		writeBackEventQueue = util.NewBlockQueueWithName("writeBackEventQueue", conf.WriteBackEventQueueSize)

		sqlLoadQueue = util.NewBlockQueueWithName(fmt.Sprintf("sqlLoad"), conf.SqlLoadEventQueueSize)
		for i := 0; i < conf.SqlLoadPoolSize; i++ {
			db, _ := pgOpen(host, port, dbname, user, password)
			go sqlRoutine(sqlLoadQueue, newSqlLoader(db)) //newSqlLoader(conf.SqlLoadPipeLineSize, host, port, dbname, user, password))
		}

		sqlUpdateQueue = make([]*util.BlockQueue, conf.SqlUpdatePoolSize)
		for i := 0; i < conf.SqlUpdatePoolSize; i++ {
			writeBackWG.Add(1)
			name := fmt.Sprintf("sqlUpdater:%d", i)
			sqlUpdateQueue[i] = util.NewBlockQueueWithName(name, conf.SqlUpdateEventQueueSize)
			db, _ := pgOpen(host, port, dbname, user, password)
			go sqlRoutine(sqlUpdateQueue[i], newSqlUpdater(name, db)) //newSqlUpdater(name, conf.SqlUpdatePipeLineSize, host, port, dbname, user, password))
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
