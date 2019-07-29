package flyfish

import (
	"flyfish/conf"
	"flyfish/errcode"
	"flyfish/proto"
	"sync/atomic"
	"time"
)

type cmdProcessorRedisCache struct {
}

func (this cmdProcessorRedisCache) processGet(ckey *cacheKey, ctx *processContext, cmd *command) bool {
	Debugln("processSet", cmd.uniKey)
	if ckey.status == cache_missing {
		cmd.reply(errcode.ERR_NOTFOUND, nil, -1)
		return false
	} else {
		ctx.redisFlag = redis_get
		ctx.commands = append(ctx.commands, cmd)
		ctx.fields["__version__"] = proto.PackField("__version__", ckey.version)
		for _, v := range cmd.fields {
			ctx.fields[v.GetName()] = v
		}
		return true
	}
}

func (this cmdProcessorRedisCache) processSet(ckey *cacheKey, ctx *processContext, cmd *command) bool {
	Debugln("processSet", cmd.uniKey)
	if nil != cmd.version && ckey.status != cache_new && *cmd.version != ckey.version {
		cmd.reply(errcode.ERR_VERSION, nil, ckey.version)
		return false
	} else {
		ctx.commands = append(ctx.commands, cmd)
		if ckey.status == cache_ok || ckey.status == cache_missing {
			//添加新的版本号
			ctx.fields["__version__"] = proto.PackField("__version__", ckey.version+1)
			if ckey.status == cache_ok {
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

func (this cmdProcessorRedisCache) processSetNx(ckey *cacheKey, ctx *processContext, cmd *command) bool {
	Debugln("processSetNx", cmd.uniKey)
	if ckey.status == cache_ok {
		//记录已经存在，不能再设置
		cmd.reply(errcode.ERR_KEY_EXIST, nil, ckey.version)
		return false
	} else {

		ctx.commands = append(ctx.commands, cmd)

		if ckey.status == cache_missing {
			ctx.fields["__version__"] = proto.PackField("__version__", ckey.version+1)
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

func (this cmdProcessorRedisCache) processCompareAndSet(ckey *cacheKey, ctx *processContext, cmd *command) bool {

	Debugln("processCompareAndSet", cmd.uniKey)

	if ckey.status == cache_missing {
		cmd.reply(errcode.ERR_NOTFOUND, nil, -1)
		return false
	} else {

		ctx.commands = append(ctx.commands, cmd)

		if ckey.status == cache_ok {
			ctx.fields["__version__"] = proto.PackField("__version__", ckey.version+1)
			ctx.writeBackFlag = write_back_update //数据存在执行update
			ctx.redisFlag = redis_set_script
		}

		ctx.replyOnDbOk = cmd.replyOnDbOk

		return true
	}
}

func (this cmdProcessorRedisCache) processCompareAndSetNx(ckey *cacheKey, ctx *processContext, cmd *command) bool {

	Debugln("processCompareAndSetNx", cmd.uniKey)

	ctx.commands = append(ctx.commands, cmd)

	if ckey.status == cache_ok || ckey.status == cache_missing {
		ctx.fields["__version__"] = proto.PackField("__version__", ckey.version+1)
		if ckey.status == cache_ok {
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

func (this cmdProcessorRedisCache) processIncrBy(ckey *cacheKey, ctx *processContext, cmd *command) bool {

	Debugln("processIncrBy", cmd.uniKey)

	ctx.commands = append(ctx.commands, cmd)

	if ckey.status == cache_ok || ckey.status == cache_missing {
		ctx.fields["__version__"] = proto.PackField("__version__", ckey.version+1)
		if ckey.status == cache_ok {
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

func (this cmdProcessorRedisCache) processDecrBy(ckey *cacheKey, ctx *processContext, cmd *command) bool {

	Debugln("processDecrBy", cmd.uniKey)

	ctx.commands = append(ctx.commands, cmd)

	if ckey.status == cache_ok || ckey.status == cache_missing {
		ctx.fields["__version__"] = proto.PackField("__version__", ckey.version+1)
		if ckey.status == cache_ok {
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

func (this cmdProcessorRedisCache) processDel(ckey *cacheKey, ctx *processContext, cmd *command) bool {

	Debugln("processDel", cmd.uniKey)

	if ckey.status == cache_missing {
		cmd.reply(errcode.ERR_NOTFOUND, nil, -1)
		return false
	} else {
		if nil != cmd.version && ckey.status == cache_ok && *cmd.version != ckey.version {
			cmd.reply(errcode.ERR_VERSION, nil, ckey.version)
			return false
		} else {
			if nil != cmd.version {
				ctx.fields["__version__"] = proto.PackField("__version__", *cmd.version)
			} else {
				ctx.fields["__version__"] = proto.PackField("__version__", ckey.version)
			}
			ctx.writeBackFlag = write_back_delete
			ctx.redisFlag = redis_del
			ctx.replyOnDbOk = cmd.replyOnDbOk
			ctx.commands = append(ctx.commands, cmd)
			return true
		}
	}
}

func (this cmdProcessorRedisCache) processCmd(ckey *cacheKey, fromClient bool) {
	ckey.mtx.Lock()

	if !fromClient {
		ckey.unlockCmdQueue()
	}

	if ckey.cmdQueueLocked || ckey.cmdQueue.Len() == 0 {
		ckey.mtx.Unlock()
		return
	}

	cmdQueue := ckey.cmdQueue
	e := cmdQueue.Front()

	if nil == e {
		ckey.mtx.Unlock()
		return
	}

	ctx := &processContext{
		commands: []*command{},
		fields:   map[string]*proto.Field{},
	}

	lastCmdType := cmdNone

	now := time.Now()

	for ; nil != e; e = cmdQueue.Front() {
		cmd := e.Value.(*command)
		if now.After(cmd.deadline) {
			//已经超时
			cmdQueue.Remove(e)
			atomic.AddInt32(&cmdCount, -1)
		} else {
			if ckey.status == cache_new && ckey.writeBacked {
				//cache_new触发sqlLoad,当前回写尚未完成，不能执行sqlLoad,所以不响应命令，让客户端请求超时
				cmdQueue.Remove(e)
				atomic.AddInt32(&cmdCount, -1)
			} else {
				ok := false
				if cmd.cmdType == cmdGet {
					if !(lastCmdType == cmdNone || lastCmdType == cmdGet) {
						break
					}
					cmdQueue.Remove(e)
					ok = this.processGet(ckey, ctx, cmd)
					if ok {
						lastCmdType = cmd.cmdType
					}
				} else if lastCmdType == cmdNone {
					cmdQueue.Remove(e)
					switch cmd.cmdType {
					case cmdSet:
						ok = this.processSet(ckey, ctx, cmd)
						break
					case cmdSetNx:
						ok = this.processSetNx(ckey, ctx, cmd)
						break
					case cmdCompareAndSet:
						ok = this.processCompareAndSet(ckey, ctx, cmd)
						break
					case cmdCompareAndSetNx:
						ok = this.processCompareAndSetNx(ckey, ctx, cmd)
						break
					case cmdIncrBy:
						ok = this.processIncrBy(ckey, ctx, cmd)
						break
					case cmdDecrBy:
						ok = this.processDecrBy(ckey, ctx, cmd)
						break
					case cmdDel:
						ok = this.processDel(ckey, ctx, cmd)
						break
					default:
						//记录日志
						atomic.AddInt32(&cmdCount, -1)
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
		}
	}

	if lastCmdType == cmdNone {
		ckey.mtx.Unlock()
		return
	}

	if fromClient && causeWriteBackCmd(lastCmdType) {
		if ckey.unit.updateQueueFull() {
			ckey.mtx.Unlock()
			if conf.GetConfig().ReplyBusyOnQueueFull {
				ctx.reply(errcode.ERR_BUSY, nil, -1)
			} else {
				atomic.AddInt32(&cmdCount, -1)
			}
			this.processCmd(ckey, fromClient)
			return
		}
	}

	fullReturn := fromClient

	ok := true

	if ckey.status == cache_ok || ckey.status == cache_missing {
		ok = ckey.unit.pushRedisReq(ctx, fullReturn)
	} else {
		ok = ckey.unit.pushSqlLoadReq(ctx, fullReturn)
	}

	if !ok {
		ckey.mtx.Unlock()
		if conf.GetConfig().ReplyBusyOnQueueFull {
			ctx.reply(errcode.ERR_BUSY, nil, -1)
		} else {
			atomic.AddInt32(&cmdCount, -1)
		}
		this.processCmd(ckey, fromClient)
	} else {
		ckey.lockCmdQueue()
		ckey.mtx.Unlock()
	}
}
