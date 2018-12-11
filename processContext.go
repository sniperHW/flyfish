package flyfish

import(
	protocol "flyfish/proto"
	"flyfish/errcode"
)

const (
	write_back_none   = 0
	write_back_insert = 1
	write_back_update = 2
	write_back_delete = 3
)

const (
	redis_none        = 0
	redis_get         = 1
	redis_set         = 2                         //直接执行set
	redis_del         = 4
	redis_set_script  = 5                         //执行设置类脚本
	redis_set_only    = 6                         //执行set,不需要执行sql回写
)

type processContext struct {
	commands       []*command                     //本次处理关联的所有命令请求
	fields         map[string]*protocol.Field        
	errno          int32
	replyed        bool                           //是否已经应道
	writeBackFlag  int                            //回写数据库类型
	redisFlag      int	
}

func (this *processContext) getCmd() *command {
	if len(this.commands) == 0 {
		panic("len(commands) == 0")
	}
	return this.commands[0]
}

func (this *processContext) getCmdType() int {
	return this.getCmd().cmdType
}

func (this *processContext) getTable() string {
	return this.getCmd().table
}

func (this *processContext) getKey() string {
	return this.getCmd().key
}

func (this *processContext) getUniKey() string {
	return this.getCmd().uniKey
}

func (this *processContext) getCacheKey() *cacheKey {
	return this.getCmd().ckey
}

func (this *processContext) getSetfields() *map[string]interface{} {
	ckey := this.getCacheKey()
	meta := ckey.meta
	ret := map[string]interface{}{} 
	for _ ,v := range(meta.fieldMetas) {
		vv,ok := this.fields[v.name]
		if ok {
			ret[v.name] = vv.GetValue()
		} else {
			ret[v.name] = v.defaultV
		}
	}
	ret["__version__"] = this.fields["__version__"].GetValue()
	return &ret
}

func (this *processContext) reply(errCode int32,fields map[string]*protocol.Field,version int64) {
	if !this.replyed {
		if len(this.commands) == 0 {
			Errorln("len(this.commands)",*this)
		}
		for _,v := range(this.commands) {
			v.reply(errCode,fields,version)
		}
		this.replyed = true
	}	
}

func (this *cacheKey) processGet(ctx *processContext,cmd *command) bool {
	if this.status == cache_missing {
		cmd.reply(errcode.ERR_NOTFOUND,nil,-1)
		return false
	} else {
		ctx.redisFlag = redis_get
		ctx.commands  = append(ctx.commands,cmd)
		ctx.fields["__version__"] = protocol.PackField("__version__",this.version)
		for _,v := range(cmd.fields) {
			ctx.fields[v.GetName()] = v
		}
		return true				
	}
}

func (this *cacheKey) processSet(ctx *processContext,cmd *command) bool {
	Debugln("processSet")
	if nil != cmd.version && this.status != cache_new && *cmd.version != this.version {
		cmd.reply(errcode.ERR_VERSION,nil,this.version)
		return false		
	} else {	
		ctx.commands = append(ctx.commands,cmd)
		if this.status == cache_ok || this.status == cache_missing {
			//添加新的版本号
			ctx.fields["__version__"] = protocol.PackField("__version__",this.version + 1)
			if this.status == cache_ok {
				ctx.writeBackFlag = write_back_update //数据存在执行update
				ctx.redisFlag = redis_set_script
			} else {
				ctx.writeBackFlag = write_back_insert //数据不存在执行insert
				ctx.redisFlag = redis_set
			}
		}
		
		for _,v := range(cmd.fields) {
			ctx.fields[v.GetName()] = v
		}
		
		return true

	}
}

func (this *cacheKey) processSetNx(ctx *processContext,cmd *command) bool {
	Debugln("processSetNx")
	if this.status == cache_ok {
		//记录已经存在，不能再设置
		cmd.reply(errcode.ERR_KEY_EXIST,nil,this.version)
		return false
	} else {	

		ctx.commands = append(ctx.commands,cmd)
		
		if this.status == cache_missing {
			ctx.fields["__version__"] = protocol.PackField("__version__",this.version + 1)
			ctx.writeBackFlag = write_back_insert //数据不存在执行insert
			ctx.redisFlag = redis_set
		}

		for _,v := range(cmd.fields) {
			ctx.fields[v.GetName()] = v
		}	

		return true
	}
}

func (this *cacheKey) processCompareAndSet(ctx *processContext,cmd *command) bool {

	Debugln("processCompareAndSet")

	if this.status == cache_missing {
		cmd.reply(errcode.ERR_NOTFOUND,nil,-1)
		return false
	} else {	

		ctx.commands = append(ctx.commands,cmd)

		if this.status == cache_ok {
			ctx.fields["__version__"] = protocol.PackField("__version__",this.version + 1)
			ctx.writeBackFlag = write_back_update //数据存在执行update
			ctx.redisFlag = redis_set_script
		}

		return true
	}
}

func (this *cacheKey) processCompareAndSetNx(ctx *processContext,cmd *command) bool {

	ctx.commands = append(ctx.commands,cmd)

	if this.status == cache_ok || this.status == cache_missing {
		ctx.fields["__version__"] = protocol.PackField("__version__",this.version + 1)
		if this.status == cache_ok {
			ctx.writeBackFlag = write_back_update //数据存在执行update
			ctx.redisFlag = redis_set_script
		} else {
			ctx.fields[cmd.cns.newV.GetName()] = cmd.cns.newV
			ctx.writeBackFlag = write_back_insert //数据不存在执行insert
			ctx.redisFlag = redis_set
		}
	}

	return true
}

func (this *cacheKey) processIncrBy(ctx *processContext,cmd *command) bool {

	Debugln("processIncrBy")

	ctx.commands = append(ctx.commands,cmd)

	if this.status == cache_ok || this.status == cache_missing {
		ctx.fields["__version__"] = protocol.PackField("__version__",this.version + 1)
		if this.status == cache_ok {
			ctx.writeBackFlag = write_back_update //数据存在执行update
			ctx.redisFlag = redis_set_script
		} else {
			ctx.writeBackFlag = write_back_insert //数据不存在执行insert
			ctx.fields[cmd.incrDecr.GetName()] = cmd.incrDecr
			ctx.redisFlag = redis_set
		}
	}
	return true
}

func (this *cacheKey) processDecrBy(ctx *processContext,cmd *command) bool {

	Debugln("processDecrBy")

	ctx.commands = append(ctx.commands,cmd)

	if this.status == cache_ok || this.status == cache_missing {
		ctx.fields["__version__"] = protocol.PackField("__version__",this.version + 1)
		if this.status == cache_ok {
			ctx.writeBackFlag = write_back_update //数据存在执行update
			ctx.redisFlag = redis_set_script			
		} else {
			newV := 0 - cmd.incrDecr.GetInt()
			ctx.fields[cmd.incrDecr.GetName()] = protocol.PackField(cmd.incrDecr.GetName(),newV)			
			ctx.writeBackFlag = write_back_insert //数据不存在执行insert
			ctx.redisFlag = redis_set
		}
	}
	return true
}

func (this *cacheKey) processDel(ctx *processContext,cmd *command) bool {
	if this.status == cache_missing {
		cmd.reply(errcode.ERR_NOTFOUND,nil,-1)
		return false
	} else {
		if nil != cmd.version && this.status == cache_ok && *cmd.version != this.version {
			cmd.reply(errcode.ERR_VERSION,nil,this.version)
			return false		
		} else {
			if nil != cmd.version {
				ctx.fields["__version__"] = protocol.PackField("__version__",*cmd.version)
			} else {
				ctx.fields["__version__"] = protocol.PackField("__version__",this.version)
			}
			ctx.writeBackFlag = write_back_delete
			ctx.commands = append(ctx.commands,cmd)
			return true
		}
	}
}

func (this *cacheKey) process() {
	if this.locked || this.cmdQueue.size == 0 {
		return
	} else {
		Debugln("process",this.uniKey)
	}

	cmdQueue := &this.cmdQueue
	cmd := cmdQueue.Head()

	if nil == cmd {
		Debugln("cmdQueue empty",this.uniKey)
		return
	}

	ctx := &processContext {
		commands : []*command{},
		fields   : map[string]*protocol.Field{},
	}

	lastCmdType := cmdNone

	for ; nil != cmd; cmd = cmdQueue.Head() {
		ok := false 
		Debugln(cmd.cmdType,lastCmdType)
		if cmd.cmdType == cmdGet {
			if !(lastCmdType == cmdNone || lastCmdType == cmdGet) {
				break
			}
			cmdQueue.Pop()
			ok = this.processGet(ctx,cmd)
			if ok {
				lastCmdType = cmd.cmdType
			}			
		} else if lastCmdType == cmdNone {
			Debugln("here",cmd.cmdType)
			cmdQueue.Pop()
			switch cmd.cmdType {
			case cmdSet:
				ok = this.processSet(ctx,cmd)				
				break
			case cmdSetNx:
				ok = this.processSetNx(ctx,cmd)
				break
			case cmdCompareAndSet:
				ok = this.processCompareAndSet(ctx,cmd)
				break
			case cmdCompareAndSetNx:
				ok = this.processCompareAndSetNx(ctx,cmd)
				break
			case cmdIncrBy:
				ok = this.processIncrBy(ctx,cmd)
				break
			case cmdDecrBy:
				ok = this.processDecrBy(ctx,cmd)
				break
			case cmdDel:
				ok = this.processDel(ctx,cmd)
				break
			default:
				//记录日志
				break
			}
				
			if ok {
				lastCmdType = cmd.cmdType
				break
			}

		} else {
			break
		}
	}

	if lastCmdType == cmdNone {
		Debugln("lastCmdType == cmdNone")
		return
	}

	this.locked = true

	if this.status == cache_ok || this.status == cache_missing {
		if lastCmdType == cmdGet {
			ctx.redisFlag = redis_get
		} else if lastCmdType == cmdDel {
			ctx.redisFlag = redis_del
		} else if this.status == cache_ok {
			ctx.redisFlag = redis_set_script
		} else {
			ctx.redisFlag = redis_set
		}

		//投递redis请求
		pushRedis(ctx)
	} else {
		//投递sql请求
		pushSQLLoad(ctx)
	}
}
