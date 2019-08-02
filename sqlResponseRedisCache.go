package flyfish

import (
	"github.com/sniperHW/flyfish/errcode"
	"github.com/sniperHW/flyfish/proto"
)

type sqlResponseRedisCache struct {
}

func (this sqlResponseRedisCache) onSqlNotFound(ctx *processContext) {
	Debugln("onSqlNotFound key", ctx.getUniKey())
	cmdType := ctx.getCmdType()
	ckey := ctx.getCacheKey()
	if cmdType == cmdGet || cmdType == cmdDel || cmdType == cmdCompareAndSet {
		ctx.reply(errcode.ERR_NOTFOUND, nil, -1)
		ckey.setMissing()
		ckey.processQueueCmd()
	} else {
		/*   set操作，数据库不存在的情况
		 *   先写入到redis,redis写入成功后回写sql(设置回写类型insert)
		 */
		meta := ckey.getMeta()
		for _, v := range meta.fieldMetas {
			ctx.fields[v.name] = proto.PackField(v.name, v.defaultV)
		}

		cmd := ctx.getCmd()
		if cmdType == cmdCompareAndSetNx {
			ctx.fields[cmd.cns.newV.GetName()] = cmd.cns.newV
		} else if cmdType == cmdIncrBy {
			oldV := cmd.incrDecr
			newV := proto.PackField(cmd.incrDecr.GetName(), oldV.GetInt()+cmd.incrDecr.GetInt())
			ctx.fields[cmd.incrDecr.GetName()] = newV
		} else if cmdType == cmdDecrBy {
			oldV := cmd.incrDecr
			newV := proto.PackField(cmd.incrDecr.GetName(), oldV.GetInt()-cmd.incrDecr.GetInt())
			ctx.fields[cmd.incrDecr.GetName()] = newV
		} else if cmdType == cmdSet {
			for _, v := range cmd.fields {
				ctx.fields[v.GetName()] = v
			}
		}
		ctx.fields["__version__"] = proto.PackField("__version__", 1)
		ctx.writeBackFlag = write_back_insert

		ctx.redisFlag = redis_set
		ckey.unit.pushRedisReq(ctx)
	}
}

func (this sqlResponseRedisCache) onSqlLoadOKGet(ctx *processContext) {
	ctx.redisFlag = redis_set_only
	ctx.getCacheKey().unit.pushRedisReq(ctx)
}

/*
*   设置类命令簇
 */
func (this sqlResponseRedisCache) onSqlLoadOKSet(ctx *processContext) {
	version := ctx.fields["__version__"].GetInt()
	cmd := ctx.commands[0]
	cmdType := cmd.cmdType
	if cmdType == cmdSet {
		if nil != cmd.version && *cmd.version != version {
			//版本号不对
			ctx.reply(errcode.ERR_VERSION, nil, version)
			ctx.redisFlag = redis_set_only
		} else {
			//变更需要将版本号+1
			for _, v := range cmd.fields {
				ctx.fields[v.GetName()] = v
			}
			ctx.fields["__version__"] = proto.PackField("__version__", version+1)
			ctx.writeBackFlag = write_back_update //sql中存在,使用update回写
			ctx.redisFlag = redis_set
		}
	} else if cmdType == cmdCompareAndSet || cmdType == cmdCompareAndSetNx {
		dbV := ctx.fields[cmd.cns.oldV.GetName()]
		if !dbV.Equal(cmd.cns.oldV) {
			ctx.reply(errcode.ERR_NOT_EQUAL, ctx.fields, version)
			ctx.redisFlag = redis_set_only
		} else {
			ctx.fields["__version__"] = proto.PackField("__version__", version+1)
			ctx.fields[cmd.cns.oldV.GetName()] = cmd.cns.newV
			ctx.writeBackFlag = write_back_update //sql中存在,使用update回写
			ctx.redisFlag = redis_set
		}
	} else if cmdType == cmdSetNx {
		ctx.reply(errcode.ERR_KEY_EXIST, nil, version)
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
		ctx.fields["__version__"] = proto.PackField("__version__", version+1)
		ctx.writeBackFlag = write_back_update //sql中存在,使用update回写
		ctx.redisFlag = redis_set
	}

	ctx.getCacheKey().unit.pushRedisReq(ctx)

}

func (this sqlResponseRedisCache) onSqlLoadOKDel(ctx *processContext) {
	var errCode int32
	version := ctx.fields["__version__"].GetInt()
	cmd := ctx.commands[0]
	ckey := ctx.getCacheKey()
	if nil != cmd.version && *cmd.version != version {
		//版本号不对
		errCode = errcode.ERR_VERSION
	} else {
		ctx.writeBackFlag = write_back_delete
		ckey.unit.doWriteBack(ctx)
		errCode = errcode.ERR_OK
	}

	ctx.reply(errCode, nil, version)
	if errCode == errcode.ERR_OK {
		ckey.setMissing()
	}
	ckey.processQueueCmd()
}

func (this sqlResponseRedisCache) onSqlLoadOK(ctx *processContext) {
	cmdType := ctx.getCmdType()
	if cmdType == cmdGet {
		this.onSqlLoadOKGet(ctx)
	} else if isSetCmd(cmdType) {
		this.onSqlLoadOKSet(ctx)
	} else if cmdType == cmdDel {
		this.onSqlLoadOKDel(ctx)
	} else {
		//记录日志
	}
}

func (this sqlResponseRedisCache) onSqlResp(ctx *processContext, errno int32) {

	Debugln("onSqlResp", ctx.getUniKey(), ctx.getCmdType(), errno)

	if errno == errcode.ERR_OK {
		this.onSqlLoadOK(ctx)
	} else if errno == errcode.ERR_NOTFOUND {
		this.onSqlNotFound(ctx)
	} else {
		onSqlExecError(ctx)
	}
}

func (this sqlResponseRedisCache) onSqlWriteBackResp(ctx *processContext, errno int32) {

	/*
		Debugln("onSqlWriteBackResp", ctx.getUniKey(), ctx.getCmdType(), errno)

		ckey := ctx.getCacheKey()
		if errno == errcode.ERR_OK {
			version := ctx.fields["__version__"].GetInt()
			ctx.reply(errno, nil, version)
		} else {
			ctx.reply(errno, nil, -1)
			//将redis中缓存作废
			ckey.setMissing()

			ckey.unit.pushRedisReq(&processContext{
				commands: []*command{&command{
					uniKey: ckey.uniKey,
					ckey:   ckey,
				}},
				redisFlag: redis_kick,
			})
		}

		ckey.processQueueCmd()
	*/

}
