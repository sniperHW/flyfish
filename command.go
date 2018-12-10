package flyfish

import(
	"github.com/sniperHW/kendynet/event"
	protocol "flyfish/proto"
	//"strconv"
	"fmt"
	"time"
	//"ff/conf"
	"flyfish/errcode"
)

const (
	cmdNone   			= 0
	cmdGet    			= 1
	cmdSet    			= 2
	cmdSetNx  			= 3
	cmdCompareAndSet 	= 4
	cmdCompareAndSetNx  = 5
	cmdDel    			= 6
	cmdIncrBy 			= 7
	cmdDecrBy 			= 8
)

var mainQueue *event.EventQueue

func isSetCmd(cmd int) bool {
	return cmd == cmdSet || cmd == cmdSetNx || cmd == cmdCompareAndSet || cmd == cmdCompareAndSetNx || cmd == cmdIncrBy || cmd == cmdDecrBy
}

//命令回复器
type replyer interface {
	reply(errCode int32,fields map[string]*protocol.Field,version int64)
}

type cnsSt struct {
	oldV   *protocol.Field
	newV   *protocol.Field
}

//来自客户端的一条命令请求
type command struct {
	next        *command
	cmdType     int
	rpyer       replyer
	table       string
	key         string
	uniKey      string                                 //table+key
	version     *int64
	ckey        *cacheKey
	fields      map[string]*protocol.Field             //for get/set
	cns         *cnsSt                                 //for compareAndSet
	incrDecr    *protocol.Field                        //for incr/decr 
}


func (this *command) reply(errCode int32,fields map[string]*protocol.Field,version int64) {
	this.rpyer.reply(errCode,fields,version)
}


func pushCommand(cmd *command) {

	meta := getMetaByTable(cmd.table)

	if nil == meta {
		cmd.reply(errcode.ERR_INVAILD_TABLE,nil,-1)
		return
	}

	if cmd.cmdType == cmdGet && !meta.checkGet(cmd.fields) {
		cmd.reply(errcode.ERR_INVAILD_FIELD,nil,-1)
		return
	}

	if (cmd.cmdType == cmdCompareAndSet || cmd.cmdType == cmdCompareAndSetNx) && 
	   !meta.checkCompareAndSet(cmd.cns.newV,cmd.cns.oldV) {
		cmd.reply(errcode.ERR_INVAILD_FIELD,nil,-1)
		return			
	}
	
	if (cmd.cmdType == cmdSet || cmd.cmdType == cmdSetNx) && !meta.checkSet(cmd.fields) {
		cmd.reply(errcode.ERR_INVAILD_FIELD,nil,-1)
		return
	}

	if (cmd.cmdType == cmdIncrBy || cmd.cmdType == cmdDecrBy) && !meta.checkField(cmd.incrDecr) {
		cmd.reply(errcode.ERR_INVAILD_FIELD,nil,-1)
		return
	}

	mainQueue.Post(func(){
		cmd.ckey = getCacheKey(cmd.table,cmd.uniKey)	
		cmd.ckey.cmdQueue.Push(cmd)
		cmd.ckey.process()	
	})
}

func init() {
	mainQueue = event.NewEventQueue()

	go func(){
		mainQueue.Run()
	}()

	go func(){
		for {
			time.Sleep(time.Second)
			fmt.Println("keys:",len(cacheKeys),"writeBackKeys",len(writeBackRecords),"writeBackQueue_.size",pendingWB.size)
		}
	}()

}



