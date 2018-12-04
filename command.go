package flyfish

import(
	"github.com/sniperHW/kendynet/event"
	//"reflect"
	message "flyfish/proto"
	"strconv"
	"fmt"
	"time"
	"flyfish/conf"
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

func isSetCmd(cmd int) bool {
	return cmd == cmdSet || cmd == cmdSetNx || cmd == cmdCompareAndSet || cmd == cmdCompareAndSetNx || cmd == cmdIncrBy || cmd == cmdDecrBy
}

var eventQueue *event.EventQueue

type field struct {
	name    string
	value   interface{} 
}

type replyer interface {
	reply(errCode int32,fields map[string]field,version ...int64)
}


func (this *field) Tt() message.ValueType {
	if this.value == nil {
		return message.ValueType_Nil
	}

	switch this.value.(type) {
		case string:
			return message.ValueType_String
		case float64:
			return message.ValueType_Float
		case int64:
			return message.ValueType_Integer
		case uint64:
			return message.ValueType_Uinteger
		default:
			panic("invaild value type")
	}

	return message.ValueType_Nil
}

func (this *field) ToSqlStr() string {
	switch this.Tt() {
		case message.ValueType_String:
			return fmt.Sprintf("'%s'",this.value.(string)) 
		case message.ValueType_Float:
			return fmt.Sprintf("%f",this.value.(float64))
		case message.ValueType_Integer:
			return strconv.FormatInt(this.value.(int64),10)
		case message.ValueType_Uinteger:
			return strconv.FormatUint(this.value.(uint64),10)
		default:
			panic("invaild value type")
	}
	return ""
}

func (this *field) Equal(o *field) bool {
	if nil == o {
		return false
	}
	if this.Tt() != o.Tt() {
		return false
	}
	if this.Tt() == message.ValueType_String {
		return this.value.(string) == o.value.(string)
	} else if this.Tt() == message.ValueType_Float {
		return this.value.(float64) == o.value.(float64)		
	} else if this.Tt() == message.ValueType_Integer {
		r := this.value.(int64) == o.value.(int64)
		fmt.Println(r)
		return r		
	} else if this.Tt() == message.ValueType_Uinteger {
		return this.value.(uint64) == o.value.(uint64)
	} else {
		return true
	}
}


//单条命令上下文
type cmdContext struct {
	next        *cmdContext
	cmdType     int
	rpyer       replyer
	table       string
	key         string
	uniKey      string   //table+key
	version     *int64
	ckey        *cacheKey	
	fields      []field
	//for compareAndSet/compareAndSetNx
	oldV        field
	newV        field
}


func (this *cmdContext) reply(errCode int32,fields map[string]field,version ...int64) {
	this.rpyer.reply(errCode,fields,version...)
}

type keyCmdQueue struct {
	locked     bool
	head       *cmdContext
	tail       *cmdContext
	size       int      	
}


const (
	write_back_none   = 0
	write_back_insert = 1
	write_back_update = 2
	write_back_delete = 3
)

/*
*   命令处理状态机
*/
type cmdStm struct {
	cmdType        int
	key            string
	table          string
	uniKey         string
	contexts       []*cmdContext           //本状态机关联的所有命令请求
	fields         map[string]field        //所有命令的字段聚合    
	errno          int32
	ckey           *cacheKey
	version        *int64
	replyed        bool
	wantField      field
	setRedisOnly   bool                    //操作只写入redis,返回后不需要执行sql回写
	writeBackFlag  int                     //回写数据库类型
	//for compareAndSet/compareAndSetNx
	oldV           field
	newV           field	
}

func (this *cmdStm) reply(errCode int32,fields map[string]field,version ...int64) {
	if !this.replyed {
		if len(this.contexts) == 0 {
			Errorln("len(this.contexts)",*this)
		}
		for _,v := range(this.contexts) {
			v.reply(errCode,fields,version...)
		}
		this.replyed = true
	}else {
		Errorln("already reply",this.uniKey)
	}	
}

func (this *keyCmdQueue) Push(context *cmdContext) {
	if nil != this.tail {
		this.tail.next = context
		this.tail = context
	} else {
		this.head = context
		this.tail = context
	}
	this.size++
}


func (this *keyCmdQueue) Pop() *cmdContext {
	//fmt.Println("cmdQueue pop")
	if nil == this.head {
		return nil
	} else {
		head := this.head
		this.head = head.next
		if this.head == nil {
			this.tail = nil
		}
		this.size--
		return head
	}
}

func (this *keyCmdQueue) Head() *cmdContext {
	return this.head
}


func (this *cacheKey) convertStr(fieldName string,value string) *field {
	m,ok := this.meta.field_types[fieldName]
	if !ok {
		return nil
	}

	if m == message.ValueType_String {
		return &field {
			name : fieldName,
			value : value,
		}
	} else if m == message.ValueType_Float {
		f, err := strconv.ParseFloat(value, 64)
		if nil != err {
			return nil
		}
		return &field {
			name : fieldName,
			value : f,
		}		
	} else if m == message.ValueType_Integer {
		i, err := strconv.ParseInt(value, 10, 64)
		if nil != err {

			return nil
		}
		return &field {
			name : fieldName,
			value : i,
		}		
	} else if m == message.ValueType_Uinteger {
		i, err := strconv.ParseUint(value, 10, 64)
		if nil != err {
			return nil
		}
		return &field {
			name : fieldName,
			value : i,
		}		
	} else {
		return nil
	}
}


func (this *cacheKey) process() {
	if this.locked {
		Debugln("locked",this.uniKey)
		return
	}else {
		Debugln("process",this.uniKey)
	}

	cmdQueue := &this.cmdQueue
	head := cmdQueue.Head()

	if nil == head {
		Debugln("cmdQueue empty",this.uniKey)
		return
	}

	stm := &cmdStm {
		key      : head.key,
		table    : head.table,
		uniKey   : head.uniKey,
		contexts : []*cmdContext{},
		ckey     : this,
		fields   : map[string]field{},
	}

	for ; nil != head; head = cmdQueue.Head() {
		if head.cmdType == cmdGet {
			if !(stm.cmdType == cmdNone || stm.cmdType == cmdGet) {
				break
			}
			cmdQueue.Pop()
			if this.status == cache_missing {
				head.reply(errcode.ERR_NOTFOUND,nil)
			} else {
				stm.cmdType = cmdGet
				stm.contexts = append(stm.contexts,head)
				for _,v := range(head.fields) {
					stm.fields[v.name] = v
				}				
			}
		} else if head.cmdType == cmdSet {
			if !(stm.cmdType == cmdNone || stm.cmdType == cmdSet) {
				break
			}
			cmdQueue.Pop()
			//set操作会变更版本号，不支持命令聚合，只能一个一个执行
			if nil != head.version && this.status != cache_new && *head.version != this.version {
				head.reply(errcode.ERR_VERSION,nil)		
			} else {	
				stm.cmdType = cmdSet
				stm.contexts = append(stm.contexts,head)
				
				if this.status == cache_ok || this.status == cache_missing {
					stm.fields["__version__"] = field {
						name  : "__version__",
						value : this.version + 1,
					}
					if this.status == cache_ok {
						stm.writeBackFlag = write_back_update //数据存在执行update
					} else {
						stm.writeBackFlag = write_back_insert //数据不存在执行insert
					}
				}
				
				for _,v := range(head.fields) {
					stm.fields[v.name] = v
				}
				break
			}

		} else if head.cmdType == cmdSetNx {
			if !(stm.cmdType == cmdNone || stm.cmdType == cmdSetNx) {
				break
			}
			cmdQueue.Pop()
			if this.status == cache_ok {
				head.reply(errcode.ERR_KEY_EXIST,nil)
			} else {	
				stm.cmdType = cmdSetNx
				stm.contexts = append(stm.contexts,head)
				
				if this.status == cache_ok || this.status == cache_missing {
					stm.fields["__version__"] = field {
						name  : "__version__",
						value : this.version + 1,
					}
					if this.status == cache_ok {
						stm.writeBackFlag = write_back_update //数据存在执行update
					} else {
						stm.writeBackFlag = write_back_insert //数据不存在执行insert
					}
				}
				
				for _,v := range(head.fields) {
					stm.fields[v.name] = v
				}
				break
			}

		} else if head.cmdType == cmdCompareAndSet {
			if !(stm.cmdType == cmdNone || stm.cmdType == cmdCompareAndSet) {
				break
			}
			cmdQueue.Pop()
			if this.status == cache_missing {
				head.reply(errcode.ERR_NOTFOUND,nil)
			} else {	
				stm.cmdType = cmdCompareAndSet
				stm.contexts = append(stm.contexts,head)
					
				if this.status == cache_ok {
					stm.fields["__version__"] = field {
						name  : "__version__",
						value : this.version + 1,
					}
					stm.writeBackFlag = write_back_update //数据存在执行update
				}
				stm.newV = head.newV
				stm.oldV = head.oldV
				break
			}

		} else if head.cmdType == cmdCompareAndSetNx {
			if !(stm.cmdType == cmdNone || stm.cmdType == cmdCompareAndSetNx) {
				break
			}
			cmdQueue.Pop()
			stm.cmdType = cmdCompareAndSetNx
			stm.contexts = append(stm.contexts,head)

			if this.status == cache_ok || this.status == cache_missing {
				stm.fields["__version__"] = field {
					name  : "__version__",
					value : this.version + 1,
				}
				if this.status == cache_ok {
					stm.writeBackFlag = write_back_update //数据存在执行update
				} else {
					stm.writeBackFlag = write_back_insert //数据不存在执行insert
				}
			}

			for _,v := range(head.fields) {
				stm.fields[v.name] = v
			}

			stm.newV = head.newV
			stm.oldV = head.oldV
			break

		}  else if head.cmdType == cmdIncrBy {
			cmdQueue.Pop()

			for _,v := range(head.fields) {
				stm.wantField = v
				break
			}

			if this.status == cache_ok || this.status == cache_missing {
				stm.fields["__version__"] = field {
					name  : "__version__",
					value : this.version + 1,
				}
			}
			stm.cmdType = cmdIncrBy
			stm.contexts = append(stm.contexts,head)	
			break
		} else if head.cmdType == cmdDecrBy {
			cmdQueue.Pop()

			for _,v := range(head.fields) {
				stm.wantField = v
				break
			}

			if this.status == cache_ok || this.status == cache_missing {
				stm.fields["__version__"] = field {
					name  : "__version__",
					value : this.version + 1,
				}
			}

			stm.cmdType = cmdDecrBy
			stm.contexts = append(stm.contexts,head)				
			break
		}  else {
			if !(stm.cmdType == cmdNone || stm.cmdType == cmdDel) {
				break
			}
			cmdQueue.Pop()
			if this.status == cache_missing {
				head.reply(errcode.ERR_NOTFOUND,nil)
			} else {
				if nil != head.version && this.status == cache_ok && *head.version != this.version {
					head.reply(errcode.ERR_VERSION,nil)		
				} else {
					stm.cmdType = cmdDel
					stm.contexts = append(stm.contexts,head)
					break
				}
			}
		}
	}

	if len(stm.contexts) == 0 {
		Debugln("stm.contexts empty",this.uniKey)
		return
	}

	this.locked = true

	if this.status == cache_ok || this.status == cache_missing {
		//投递redis请求
		pushRedis(stm)
	} else {
		//投递sql请求
		pushSQLLoad(stm)
	}

}

func onSqlNotFound(stm *cmdStm) {
	Debugln("onSqlNotFound key",stm.uniKey)
	ckey := stm.ckey
	ckey.setMissing() //设置数据不存在标记
	if stm.cmdType == cmdGet || stm.cmdType == cmdDel || stm.cmdType == cmdCompareAndSet {
		stm.reply(errcode.ERR_NOTFOUND,nil)
		ckey.locked = false
		ckey.process()		
	} else {
		/*  set操作，数据库不存在的情况
		*   先写入到redis,redis写入成功后回写sql(设置回写类型insert)
		*/
		stm.fields["__version__"] = field {
			name  : "__version__",
			value : ckey.version + 1,
		}
		stm.writeBackFlag = write_back_insert
		pushRedis(stm)
	}
}

func onSqlExecError(stm *cmdStm) {
	Debugln("onSqlExecError key",stm.uniKey)
	ckey := stm.ckey	
	ckey.locked = false
	stm.reply(errcode.ERR_SQLERROR,nil)
	ckey.process()	
}

func onSqlLoadOK(stm *cmdStm) { 
	version := stm.fields["__version__"].value.(int64)//从数据库读取到的版本号
	Debugln("onSqlLoadOK key",stm.uniKey,"version",version)
	if stm.cmdType == cmdGet {
		stm.setRedisOnly = true
		pushRedis(stm)
	} else if isSetCmd(stm.cmdType) {
		if stm.cmdType == cmdCompareAndSet || stm.cmdType == cmdCompareAndSetNx {
			dbV := stm.fields[stm.oldV.name]
			fmt.Println(dbV,stm.oldV,dbV.Tt(),stm.oldV.Tt())
			if !dbV.Equal(&stm.oldV) {
				stm.fields[stm.oldV.name] = field{
					name : stm.oldV.name,
					value : dbV.value,
				}
				stm.reply(errcode.ERR_NOT_EQUAL,stm.fields)
				stm.ckey.locked = false
				stm.ckey.process()				
			} else {
				stm.version = new(int64)
				*stm.version = version + 1
				stm.fields["__version__"] = field {
					name : "__version__",
					value : *stm.version,
				}
				stm.fields[stm.oldV.name] = stm.newV
				stm.writeBackFlag = write_back_update   //sql中存在,使用update回写
				pushRedis(stm)	
			}
		} else if stm.cmdType == cmdSetNx {
			stm.reply(errcode.ERR_KEY_EXIST,nil)
			stm.ckey.locked = false
			stm.ckey.process()
		} else if stm.cmdType == cmdSet && nil != stm.version && *stm.version != version {
			//版本号不对
			stm.reply(errcode.ERR_VERSION,nil)
			stm.ckey.locked = false
			stm.ckey.process()
		} else {
			//变更需要将版本号+1
			stm.version = new(int64)
			*stm.version = version + 1
			stm.fields["__version__"] = field {
				name : "__version__",
				value : *stm.version,
			}
			stm.writeBackFlag = write_back_update   //sql中存在,使用update回写
			pushRedis(stm)
		}
	} else if stm.cmdType == cmdDel {
		if nil != stm.version && *stm.version != version {
			//版本号不对
			stm.reply(errcode.ERR_VERSION,nil)
		} else {
			stm.ckey.setMissing()
			stm.writeBackFlag = write_back_delete
			pushSQLWriteBack(stm)
			stm.reply(errcode.ERR_OK,nil)
		}
		
		stm.ckey.locked = false
		stm.ckey.process()

	} else {
		//记录日志
	}
}

func onSqlResp(stm *cmdStm,errno int32) {
	eventQueue.Post(func(){
		if errno == errcode.ERR_OK {
			onSqlLoadOK(stm)
		} else if errno == errcode.ERR_NOTFOUND {
			onSqlNotFound(stm)
		}
	})
}

func onRedisResp(stm *cmdStm) {
	eventQueue.Post(func(){
		Debugln("onRedisResp key:",stm.uniKey,stm.errno,stm.setRedisOnly)
		ckey := stm.ckey	
		ckey.locked = false
		if stm.errno == errcode.ERR_OK {
			if stm.setRedisOnly {
				ckey.setOK(*stm.version)
			} else if isSetCmd(stm.cmdType) {
				ckey.setOK(*stm.version)
				//投递sql更新
				pushSQLWriteBack(stm)	
			} else if stm.cmdType == cmdDel {
				ckey.setMissing()
				//投递sql删除请求
				stm.writeBackFlag = write_back_delete
				pushSQLWriteBack(stm)				
			} 
		} else if stm.errno == errcode.ERR_VERSION {
			//cachekey中记录的version与redis中的不一致
			ckey.setOK(*stm.version)
		} else if stm.errno == errcode.ERR_STALE_CACHE {
			/*  redis中的数据与flyfish key不一致

			 *  将ckey重置为cache_new，强制从数据库取值刷新redis
			*/
			ckey.reset()
		}
		stm.reply(stm.errno,stm.fields,ckey.version)
		ckey.process()
	})
}

func pushCmdContext(context *cmdContext) {
	eventQueue.PostWait(func(){

		context.ckey = getCacheKey(context.table,context.uniKey)

		if nil == context.ckey {
			context.reply(errcode.ERR_INVAILD_TABLE,nil)
			return
		}

		if context.cmdType == cmdGet && !context.ckey.meta.checkGet(context.fields) {
			context.reply(errcode.ERR_INVAILD_FIELD,nil)
			return
		}

		if (context.cmdType == cmdCompareAndSet || context.cmdType == cmdCompareAndSetNx) && !context.ckey.meta.checkCompareAndSet(&context.oldV,&context.newV) {
			context.reply(errcode.ERR_INVAILD_FIELD,nil)
			return			
		}
		
		if (context.cmdType == cmdSet || context.cmdType == cmdSetNx) && !context.ckey.meta.checkSet(context.fields) {
			context.reply(errcode.ERR_INVAILD_FIELD,nil)
			return
		}

		if (context.cmdType == cmdIncrBy || context.cmdType == cmdDecrBy) && !context.ckey.meta.checkSet(context.fields) {
			context.reply(errcode.ERR_INVAILD_FIELD,nil)
			return
		}

		ckey := context.ckey
		ckey.cmdQueue.Push(context)
		ckey.process()
	})
}

func CommandClose() {
	
}

func init() {
	eventQueue = event.NewEventQueue(conf.MainEventQueueSize)
	go func(){
		eventQueue.Run()
	}()

	go func(){
		for {
			time.Sleep(time.Second)
			fmt.Println("keys:",len(cacheKeys),"writeBackKeys",len(writeBackKeys),"writeBackQueue_.size",writeBackQueue_.size)
		}
	}()

}