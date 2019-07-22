package flyfish

import (
	"flyfish/errcode"
	"flyfish/proto"
	//"sync/atomic"
	//"time"
)

const (
	write_back_none   = 0
	write_back_insert = 1
	write_back_update = 2
	write_back_delete = 3
	write_back_force  = 4
)

const (
	redis_none       = 0
	redis_get        = 1
	redis_set        = 2 //直接执行set
	redis_del        = 3
	redis_set_script = 4 //执行设置类脚本
	redis_set_only   = 5 //执行set,不需要执行sql回写
	redis_kick       = 6 //剔除cache
	redis_end        = 7
)

type processContext struct {
	commands      []*command //本次处理关联的所有命令请求
	fields        map[string]*proto.Field
	errno         int32
	replyed       bool //是否已经应道
	writeBackFlag int  //回写数据库类型
	redisFlag     int
	ping          bool
	replyOnDbOk   bool //是否在db操作完成后才返回响应
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
	for _, v := range meta.fieldMetas {
		vv, ok := this.fields[v.name]
		if ok {
			ret[v.name] = vv.GetValue()
		} else {
			ret[v.name] = v.defaultV
			this.fields[v.name] = proto.PackField(v.name, v.defaultV)
		}
	}
	ret["__version__"] = this.fields["__version__"].GetValue()
	return &ret
}

func (this *processContext) reply(errCode int32, fields map[string]*proto.Field, version int64) {
	if !this.replyed {
		if len(this.commands) == 0 {
			Errorln("len(this.commands)", *this)
		}
		for _, v := range this.commands {
			v.reply(errCode, fields, version)
		}
		this.replyed = true
	}
}

func (this *cacheKey) processGet(ctx *processContext, cmd *command) bool {
	Debugln("processSet", cmd.uniKey)
	if this.status == cache_missing {
		cmd.reply(errcode.ERR_NOTFOUND, nil, -1)
		return false
	} else {
		ctx.redisFlag = redis_get
		ctx.commands = append(ctx.commands, cmd)
		ctx.fields["__version__"] = proto.PackField("__version__", this.version)
		for _, v := range cmd.fields {
			ctx.fields[v.GetName()] = v
		}
		return true
	}
}

func (this *cacheKey) processSet(ctx *processContext, cmd *command) bool {
	Debugln("processSet", cmd.uniKey)
	if nil != cmd.version && this.status != cache_new && *cmd.version != this.version {
		cmd.reply(errcode.ERR_VERSION, nil, this.version)
		return false
	} else {
		ctx.commands = append(ctx.commands, cmd)
		if this.status == cache_ok || this.status == cache_missing {
			//添加新的版本号
			ctx.fields["__version__"] = proto.PackField("__version__", this.version+1)
			if this.status == cache_ok {
				ctx.writeBackFlag = write_back_update //数据存在执行update
				/*
				 *    使用redis_set_script的原因
				 *    redis设置为缓存不写磁盘模式，一旦redis崩溃后重启内存中数据将会丢失，使得flyfish与redis不一致(flyfish标记cache_ok,redis中却没有)
				 *    为了能正确处理这种情况，需要执行lua脚本进行一次验证，如果验证不通过重置flyfish的标记
				 */
				ctx.redisFlag = redis_set_script
				//ctx.redisFlag = redis_set
			} else {
				ctx.writeBackFlag = write_back_insert //数据不存在执行insert
				ctx.redisFlag = redis_set
			}
		}

		for _, v := range cmd.fields {
			ctx.fields[v.GetName()] = v
		}
		ctx.replyOnDbOk = cmd.replyOnDbOk

		return true

	}
}

func (this *cacheKey) processSetNx(ctx *processContext, cmd *command) bool {
	Debugln("processSetNx", cmd.uniKey)
	if this.status == cache_ok {
		//记录已经存在，不能再设置
		cmd.reply(errcode.ERR_KEY_EXIST, nil, this.version)
		return false
	} else {

		ctx.commands = append(ctx.commands, cmd)

		if this.status == cache_missing {
			ctx.fields["__version__"] = proto.PackField("__version__", this.version+1)
			ctx.writeBackFlag = write_back_insert //数据不存在执行insert
			ctx.redisFlag = redis_set
		}

		for _, v := range cmd.fields {
			ctx.fields[v.GetName()] = v
		}

		ctx.replyOnDbOk = cmd.replyOnDbOk

		return true
	}
}

func (this *cacheKey) processCompareAndSet(ctx *processContext, cmd *command) bool {

	Debugln("processCompareAndSet", cmd.uniKey)

	if this.status == cache_missing {
		cmd.reply(errcode.ERR_NOTFOUND, nil, -1)
		return false
	} else {

		ctx.commands = append(ctx.commands, cmd)

		if this.status == cache_ok {
			ctx.fields["__version__"] = proto.PackField("__version__", this.version+1)
			ctx.writeBackFlag = write_back_update //数据存在执行update
			ctx.redisFlag = redis_set_script
		}

		ctx.replyOnDbOk = cmd.replyOnDbOk

		return true
	}
}

func (this *cacheKey) processCompareAndSetNx(ctx *processContext, cmd *command) bool {

	Debugln("processCompareAndSetNx", cmd.uniKey)

	ctx.commands = append(ctx.commands, cmd)

	if this.status == cache_ok || this.status == cache_missing {
		ctx.fields["__version__"] = proto.PackField("__version__", this.version+1)
		if this.status == cache_ok {
			ctx.writeBackFlag = write_back_update //数据存在执行update
			ctx.redisFlag = redis_set_script
		} else {
			ctx.fields[cmd.cns.newV.GetName()] = cmd.cns.newV
			ctx.writeBackFlag = write_back_insert //数据不存在执行insert
			ctx.redisFlag = redis_set
		}
	}

	ctx.replyOnDbOk = cmd.replyOnDbOk

	return true
}

func (this *cacheKey) processIncrBy(ctx *processContext, cmd *command) bool {

	Debugln("processIncrBy", cmd.uniKey)

	ctx.commands = append(ctx.commands, cmd)

	if this.status == cache_ok || this.status == cache_missing {
		ctx.fields["__version__"] = proto.PackField("__version__", this.version+1)
		if this.status == cache_ok {
			ctx.writeBackFlag = write_back_update //数据存在执行update
			ctx.redisFlag = redis_set_script
		} else {
			ctx.writeBackFlag = write_back_insert //数据不存在执行insert
			ctx.fields[cmd.incrDecr.GetName()] = cmd.incrDecr
			ctx.redisFlag = redis_set
		}
	}
	ctx.replyOnDbOk = cmd.replyOnDbOk
	return true
}

func (this *cacheKey) processDecrBy(ctx *processContext, cmd *command) bool {

	Debugln("processDecrBy", cmd.uniKey)

	ctx.commands = append(ctx.commands, cmd)

	if this.status == cache_ok || this.status == cache_missing {
		ctx.fields["__version__"] = proto.PackField("__version__", this.version+1)
		if this.status == cache_ok {
			ctx.writeBackFlag = write_back_update //数据存在执行update
			ctx.redisFlag = redis_set_script
		} else {
			newV := 0 - cmd.incrDecr.GetInt()
			ctx.fields[cmd.incrDecr.GetName()] = proto.PackField(cmd.incrDecr.GetName(), newV)
			ctx.writeBackFlag = write_back_insert //数据不存在执行insert
			ctx.redisFlag = redis_set
		}
	}
	ctx.replyOnDbOk = cmd.replyOnDbOk
	return true
}

func (this *cacheKey) processDel(ctx *processContext, cmd *command) bool {

	Debugln("processDel", cmd.uniKey)

	if this.status == cache_missing {
		cmd.reply(errcode.ERR_NOTFOUND, nil, -1)
		return false
	} else {
		if nil != cmd.version && this.status == cache_ok && *cmd.version != this.version {
			cmd.reply(errcode.ERR_VERSION, nil, this.version)
			return false
		} else {
			if nil != cmd.version {
				ctx.fields["__version__"] = proto.PackField("__version__", *cmd.version)
			} else {
				ctx.fields["__version__"] = proto.PackField("__version__", this.version)
			}
			ctx.writeBackFlag = write_back_delete
			ctx.redisFlag = redis_del
			ctx.replyOnDbOk = cmd.replyOnDbOk
			ctx.commands = append(ctx.commands, cmd)
			return true
		}
	}
}

func (this *cacheKey) processClientCmd(cmd ...*command) {
	this.process_(true, cmd...)
}

func (this *cacheKey) process(cmd ...*command) {
	this.process_(false, cmd...)
}
