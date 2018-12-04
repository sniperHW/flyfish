package flyfish

import (
	"fmt"
	"sync"
	"github.com/sniperHW/kendynet/util"
	"flyfish/conf"
	"time"
)

var sql_once sync.Once

var sqlLoadQueue            *util.BlockQueue        //for get
var sqlUpdateQueue 			[]*util.BlockQueue      //for set/del
var writeBackKeys  			map[string]*writeBack   
var writeBackEventQueue     *util.BlockQueue
var writeBackQueue_         writeBackQueue

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

		writeBackKeys = map[string]*writeBack{}   
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