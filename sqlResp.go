package flyfish

import (
	//"fmt"
	//"sync"
	//"github.com/sniperHW/kendynet/util"
	//"flyfish/conf"
	//"time"
	"flyfish/errcode"
	protocol "flyfish/proto"
)

                                                         
func onSqlNotFound(ctx *processContext) {
	Debugln("onSqlNotFound key",ctx.getUniKey())
	cmdType := ctx.getCmdType()
	if cmdType == cmdGet || cmdType == cmdDel || cmdType == cmdCompareAndSet {
		ctx.reply(errcode.ERR_NOTFOUND,nil,-1)		
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

	mainQueue.PostNoWait(func(){
		ckey := ctx.getCacheKey()
		ckey.setMissing()	
		if cmdType == cmdGet || cmdType == cmdDel || cmdType == cmdCompareAndSet {
			ckey.locked = false
			ckey.process()
		}	
	})

}

func onSqlExecError(ctx *processContext) {
	Debugln("onSqlExecError key",ctx.getUniKey())
	ctx.reply(errcode.ERR_SQLERROR,nil,-1)
	mainQueue.PostNoWait(func(){
		ckey := ctx.getCacheKey()	
		ckey.locked = false
		ckey.process()	
	})
}

func onSqlLoadOKGet(ctx *processContext) {
	ctx.redisFlag = redis_set_only
	pushRedisNoWait(ctx)
}

/*
*   设置类命令簇
*/
func onSqlLoadOKSet(ctx *processContext) {
	version := ctx.fields["__version__"].GetInt()
	cmd := ctx.commands[0]
	ckey := ctx.getCacheKey()
	cmdType := cmd.cmdType
	if cmdType == cmdSet {
		if nil != cmd.version && *cmd.version != version {
			//版本号不对
			ctx.reply(errcode.ERR_VERSION,nil,version)
			ckey.locked = false
			ckey.process()
		} else {
			//变更需要将版本号+1
			for _,v := range(cmd.fields) {
				ctx.fields[v.GetName()] = v
			}
			ctx.fields["__version__"] = protocol.PackField("__version__",version + 1)
			ctx.writeBackFlag = write_back_update   //sql中存在,使用update回写
			ctx.redisFlag = redis_set
			pushRedisNoWait(ctx)			
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
		pushRedisNoWait(ctx)
	} else if cmdType == cmdSetNx {
		ctx.reply(errcode.ERR_KEY_EXIST,nil,version)
		ctx.redisFlag = redis_set_only
		pushRedisNoWait(ctx)
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
		pushRedisNoWait(ctx)		
	}
}

func onSqlLoadOKDel(ctx *processContext) {
	
	version := ctx.fields["__version__"].GetInt()
	cmd := ctx.commands[0]
	ok  := false
	if nil != cmd.version && *cmd.version != version {
		//版本号不对
		ctx.reply(errcode.ERR_VERSION,nil,version)
	} else {
		ok = true
		ctx.writeBackFlag = write_back_delete
		ctx.redisFlag = redis_del
		pushSQLWriteBackNoWait(ctx)
		ctx.reply(errcode.ERR_OK,nil,version)
	}
	
	mainQueue.PostNoWait(func(){
		ckey := ctx.getCacheKey()
		if ok {
			ckey.setMissing()
		}
		ckey.locked = false
		ckey.process()
	})
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