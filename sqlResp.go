package flyfish

import (
	"flyfish/errcode"
	protocol "flyfish/proto"
)

func processSqlNotFound(args []interface{}) {
	ckey := args[0].(*cacheKey)
	ckey.setMissing()	
	ckey.unlock()
	ckey.process()	
}
                                                         
func onSqlNotFound(ctx *processContext) {
	Debugln("onSqlNotFound key",ctx.getUniKey())
	cmdType := ctx.getCmdType()
	if cmdType == cmdGet || cmdType == cmdDel || cmdType == cmdCompareAndSet {
		ctx.reply(errcode.ERR_NOTFOUND,nil,-1)
		postKeyEventNoWait(ctx.getUniKey(),processSqlNotFound,ctx.getCacheKey())
		//mainQueue.PostNoWait(processSqlNotFound,ctx.getCacheKey())
	} else {
		/*  set操作，数据库不存在的情况
		*   先写入到redis,redis写入成功后回写sql(设置回写类型insert)
		*/
		cmd := ctx.getCmd()
		if cmdType == cmdCompareAndSetNx {
			ctx.fields[cmd.cns.newV.GetName()] = cmd.cns.newV
		} else if cmdType == cmdIncrBy {
			ctx.fields[cmd.incrDecr.GetName()] = cmd.incrDecr
		} else if cmdType == cmdDecrBy {
			newV := 0 - cmd.incrDecr.GetInt()
			ctx.fields[cmd.incrDecr.GetName()] = protocol.PackField(cmd.incrDecr.GetName(),newV)			
		} else if cmdType == cmdSet {
			for _,v := range(cmd.fields) {
				ctx.fields[v.GetName()] = v
			}
		}
		ctx.fields["__version__"] = protocol.PackField("__version__",1)
		ctx.writeBackFlag = write_back_insert

		ctx.redisFlag = redis_set
		pushRedisNoWait(ctx)
	}
}

func processSqlExecError(args []interface{}) {
	ckey := args[0].(*cacheKey)	
	ckey.unlock()
	ckey.process()	
}

func onSqlExecError(ctx *processContext) {
	Debugln("onSqlExecError key",ctx.getUniKey())
	ctx.reply(errcode.ERR_SQLERROR,nil,-1)
	//mainQueue.PostNoWait(processSqlExecError,ctx.getCacheKey())
	postKeyEventNoWait(ctx.getUniKey(),processSqlExecError,ctx.getCacheKey())
}

func onSqlLoadOKGet(ctx *processContext) {
	ctx.redisFlag = redis_set_only
	pushRedisNoWait(ctx)
}

func processSqlLoadOKSet(args []interface{}) {
	ckey := args[0].(*cacheKey)		
	ckey.unlock()
	ckey.process()	
}

/*
*   设置类命令簇
*/
func onSqlLoadOKSet(ctx *processContext) {
	version := ctx.fields["__version__"].GetInt()
	cmd := ctx.commands[0]
	cmdType := cmd.cmdType
	pushRedis := true
	if cmdType == cmdSet {
		if nil != cmd.version && *cmd.version != version {
			pushRedis = false
			//版本号不对
			ctx.reply(errcode.ERR_VERSION,nil,version)
			postKeyEventNoWait(ctx.getUniKey(),processSqlLoadOKSet,ctx.getCacheKey())
			//mainQueue.PostNoWait(processSqlLoadOKSet,ctx.getCacheKey())
		} else {
			//变更需要将版本号+1
			for _,v := range(cmd.fields) {
				ctx.fields[v.GetName()] = v
			}
			ctx.fields["__version__"] = protocol.PackField("__version__",version + 1)
			ctx.writeBackFlag = write_back_update   //sql中存在,使用update回写
			ctx.redisFlag = redis_set			
		}		
	} else if cmdType == cmdCompareAndSet || cmdType == cmdCompareAndSetNx {
		dbV := ctx.fields[cmd.cns.oldV.GetName()]
		if !dbV.Equal(cmd.cns.oldV) {
			ctx.reply(errcode.ERR_NOT_EQUAL,ctx.fields,version)
			ctx.redisFlag = redis_set_only			
		} else {
			ctx.fields["__version__"] = protocol.PackField("__version__",version + 1)
			ctx.fields[cmd.cns.oldV.GetName()] = cmd.cns.newV
			ctx.writeBackFlag = write_back_update   //sql中存在,使用update回写
			ctx.redisFlag = redis_set			
		}
	} else if cmdType == cmdSetNx {
		ctx.reply(errcode.ERR_KEY_EXIST,nil,version)
		ctx.redisFlag = redis_set_only
	} else {
		//cmdIncrBy/cmdDecrBy
		var newV int64
		oldV := ctx.fields[cmd.incrDecr.GetName()]
		if cmdType == cmdIncrBy {
			newV = oldV.GetInt() + cmd.incrDecr.GetInt()
		} else {
			newV = oldV.GetInt() - cmd.incrDecr.GetInt()
		}
		ctx.fields[cmd.incrDecr.GetName()].SetInt(newV)
		ctx.fields["__version__"] = protocol.PackField("__version__",version + 1)
		ctx.writeBackFlag = write_back_update   //sql中存在,使用update回写
		ctx.redisFlag = redis_set	
	}

	if pushRedis {
		pushRedisNoWait(ctx)
	}
}

func processSqlLoadOKDel(args []interface{}) {
	ckey := args[0].(*cacheKey)
	errCode := args[1].(int32)
	if errCode == errcode.ERR_OK {
		ckey.setMissing()
	}
	ckey.unlock()
	ckey.process()	
}

func onSqlLoadOKDel(ctx *processContext) {
	var errCode int32
	version := ctx.fields["__version__"].GetInt()
	cmd := ctx.commands[0]
	if nil != cmd.version && *cmd.version != version {
		//版本号不对
		errCode = errcode.ERR_VERSION
	} else {
		ctx.writeBackFlag = write_back_delete
		ctx.redisFlag = redis_del
		pushSQLWriteBackNoWait(ctx)
		errCode = errcode.ERR_OK
	}

	ctx.reply(errCode,nil,version)

	postKeyEventNoWait(ctx.getUniKey(),processSqlLoadOKDel,ctx.getCacheKey(),errCode)	

	//mainQueue.PostNoWait(processSqlLoadOKDel,ctx.getCacheKey(),errCode)
}

func onSqlLoadOK(ctx *processContext) { 
	cmdType := ctx.getCmdType()
	if cmdType == cmdGet {
		onSqlLoadOKGet(ctx)
	} else if isSetCmd(cmdType) {
		onSqlLoadOKSet(ctx)
	} else if cmdType == cmdDel {
		onSqlLoadOKDel(ctx)
	} else {
		//记录日志
	}
}

func onSqlResp(ctx *processContext,errno int32) {
	Debugln("onSqlResp",errno)
	if errno == errcode.ERR_OK {
		onSqlLoadOK(ctx)
	} else if errno == errcode.ERR_NOTFOUND {
		onSqlNotFound(ctx)
	} else {
		onSqlExecError(ctx)
	}	
}