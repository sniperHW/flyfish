package flyfish

import (
	"container/list"
	"flyfish/conf"
	"fmt"
	"github.com/sniperHW/kendynet/util"
	"sync"
	"time"
)

var (
	sql_once            sync.Once
	sqlLoadQueue        *util.BlockQueue   //for get
	sqlUpdateQueue      []*util.BlockQueue //for set/del
	writeBackRecords    map[string]*record
	writeBackEventQueue *util.BlockQueue
	pendingWB           *list.List
	writeBackBarrior_   writeBackBarrior
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
		writeBackBarrior_.add()
	}
}

func pushSQLWriteBackNoWait(ctx *processContext) {
	ckey := ctx.getCacheKey()
	ckey.setWriteBack()
	if nil == writeBackEventQueue.AddNoWait(ctx) {
		writeBackBarrior_.add()
	}
}

func pushSQLLoad_(ctx *processContext, noWait bool) {
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

	} else {
		if noWait {
			sqlLoadQueue.AddNoWait(ctx)
		} else {
			sqlLoadQueue.Add(ctx)
		}
	}
}

func pushSQLLoadNoWait(ctx *processContext) {
	pushSQLLoad_(ctx, true)
}

func pushSQLLoad(ctx *processContext) {
	pushSQLLoad_(ctx, false)
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

		pendingWB = list.New()

		writeBackBarrior_.cond = sync.NewCond(&writeBackBarrior_.mtx)

		writeBackRecords = map[string]*record{}
		writeBackEventQueue = util.NewBlockQueueWithName("writeBackEventQueue", conf.WriteBackEventQueueSize)

		sqlLoadQueue = util.NewBlockQueueWithName(fmt.Sprintf("sqlLoad"), conf.SqlLoadEventQueueSize)
		for i := 0; i < conf.SqlLoadPoolSize; i++ {
			go sqlRoutine(sqlLoadQueue, newSqlLoader(conf.SqlLoadPipeLineSize, host, port, dbname, user, password))
		}

		sqlUpdateQueue = make([]*util.BlockQueue, conf.SqlUpdatePoolSize)
		for i := 0; i < conf.SqlUpdatePoolSize; i++ {
			writeBackWG.Add(1)
			name := fmt.Sprintf("sqlUpdater:%d", i)
			sqlUpdateQueue[i] = util.NewBlockQueueWithName(name, conf.SqlUpdateEventQueueSize)
			go sqlRoutine(sqlUpdateQueue[i], newSqlUpdater(name, conf.SqlUpdatePipeLineSize, host, port, dbname, user, password))
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
