package flyfish

import (
	"fmt"
	"sync"
	"github.com/sniperHW/kendynet/util"
	"flyfish/conf"
	protocol "flyfish/proto"
	"time"
	"container/list"
)

var sql_once sync.Once

var sqlLoadQueue            *util.BlockQueue        //for get
var sqlUpdateQueue 			[]*util.BlockQueue      //for set/del
var writeBackRecords  	    map[string]*record   
var writeBackEventQueue     *util.BlockQueue
var pendingWB               *list.List//pendingWriteBack

func prepareRecord(ctx *processContext) *record {
	uniKey := ctx.getUniKey()
	wb := &record{
		writeBackFlag : ctx.writeBackFlag,
		key     : ctx.getKey(),
		table   : ctx.getTable(),
		uniKey  : uniKey,
		ckey    : ctx.getCacheKey(),
		fields  : map[string]*protocol.Field{}, 
	}
	if wb.writeBackFlag == write_back_insert || wb.writeBackFlag == write_back_update {
		wb.fields = ctx.fields
	}
	return wb	
}

func pushSQLWriteBack(ctx *processContext) {
	if conf.WriteBackDelay > 0 {
		//延迟回写
		writeBackEventQueue.Add(ctx)
	} else {
		//直接回写
		uniKey := ctx.getUniKey()
		hash := StringHash(uniKey)
		sqlUpdateQueue[hash%conf.SqlUpdatePoolSize].Add(prepareRecord(ctx))		
	}
}

func pushSQLWriteBackNoWait(ctx *processContext) {
	if conf.WriteBackDelay > 0 {
		//延迟回写
		writeBackEventQueue.AddNoWait(ctx)
	} else {
		//直接回写
		uniKey := ctx.getUniKey()
		hash := StringHash(uniKey)
		sqlUpdateQueue[hash%conf.SqlUpdatePoolSize].AddNoWait(prepareRecord(ctx))		
	}	
}

func pushSQLLoad(ctx *processContext) {
	Debugln("pushSQLLoad")
	sqlLoadQueue.Add(ctx)
}

func pushSQLLoadNoWait(ctx *processContext) {
	sqlLoadQueue.AddNoWait(ctx)
}

type sqlPipeliner interface {
	append(v interface{})
	exec()
}          

func sqlRoutine(queue *util.BlockQueue,pipeliner sqlPipeliner) {
	for {
		closed, localList := queue.Get()
		for _,v := range(localList) {
			pipeliner.append(v)
		}
		pipeliner.exec()
		if closed {
			return
		}
	}
}

func SqlClose() {
	
}

func SQLInit(dbname string,user string,password string) bool {
	sql_once.Do(func() {

		pendingWB = list.New()
		writeBackRecords = map[string]*record{}   
		writeBackEventQueue = util.NewBlockQueueWithName("writeBackEventQueue",conf.WriteBackEventQueueSize)

		sqlLoadQueue = util.NewBlockQueueWithName(fmt.Sprintf("sqlLoad"),conf.SqlEventQueueSize)
		for i := 0; i < conf.SqlLoadPoolSize; i++ {
			go sqlRoutine(sqlLoadQueue,newSqlLoader(conf.SqlLoadPipeLineSize,dbname,user,password))
		}	

		sqlUpdateQueue = make([]*util.BlockQueue,conf.SqlUpdatePoolSize)
		for i := 0; i < conf.SqlUpdatePoolSize; i++ {
			sqlUpdateQueue[i] = util.NewBlockQueueWithName(fmt.Sprintf("sqlUpdater:%d",i),conf.SqlEventQueueSize)
			go sqlRoutine(sqlUpdateQueue[i],newSqlUpdater(conf.SqlUpdatePipeLineSize,dbname,user,password))
		}

		go writeBackRoutine()

		go func(){
			for {
				time.Sleep(time.Second)
				writeBackEventQueue.Add(struct{}{})
			}
		}()


		/*go func(){
			for {
				time.Sleep(time.Second)
				fmt.Println("---------------sqlQueryer-------------")
				for _,v := range(sqlQueryQueue) {
					fmt.Println(v.Len())
				}
				fmt.Println("---------------sqlUpdateQueue-------------")
				for _,v := range(sqlUpdateQueue) {
					fmt.Println(v.Len())
				}
			}
		}()*/

	})
	
	return true
}