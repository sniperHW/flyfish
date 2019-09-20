package server

import (
	"github.com/sniperHW/flyfish/conf"
	"github.com/sniperHW/flyfish/errcode"
	"github.com/sniperHW/flyfish/proto"
	"time"
)

func processGet(ckey *cacheKey, cmd *command, ctx *cmdContext) {
	Debugln("processGet", cmd.uniKey)
	if ckey.status != cache_missing {
		if ckey.status == cache_ok {
			ctx.fields = ckey.values
			ctx.version = ckey.version
		} else {
			ctx.fields = map[string]*proto.Field{}
		}
	}
	//连续的get请求可以合并到同一个ctx钟
	ctx.commands = append(ctx.commands, cmd)
}

func processSet(ckey *cacheKey, cmd *command, ctx *cmdContext) {
	Debugln("processSet", cmd.uniKey)
	if nil != cmd.version {
		if ckey.status == cache_missing {
			cmd.reply(errcode.ERR_NOTFOUND, nil, -1)
			return
		}

		if ckey.status == cache_ok && *cmd.version != ckey.version {
			cmd.reply(errcode.ERR_VERSION, nil, ckey.version)
			return
		}
	}

	ctx.commands = append(ctx.commands, cmd)

	if ckey.status == cache_ok {
		ctx.writeBackFlag = write_back_update //数据存在执行update
		ctx.fields = cmd.fields
	} else if ckey.status == cache_missing {
		ctx.writeBackFlag = write_back_insert //数据不存在执行insert
		ctx.fields = cmd.fields
	} else {
		ctx.fields = map[string]*proto.Field{}
	}

	return
}

func processSetNx(ckey *cacheKey, cmd *command, ctx *cmdContext) {
	Debugln("processSetNx", cmd.uniKey)
	if ckey.status == cache_ok {
		//记录已经存在，不能再设置
		cmd.reply(errcode.ERR_KEY_EXIST, nil, ckey.version)
		return
	}

	ctx.commands = append(ctx.commands, cmd)

	if ckey.status == cache_missing {
		ctx.writeBackFlag = write_back_insert //数据不存在执行insert
		ctx.fields = cmd.fields
	} else {
		ctx.fields = map[string]*proto.Field{}
	}

	return
}

func processCompareAndSet(ckey *cacheKey, cmd *command, ctx *cmdContext) {

	Debugln("processCompareAndSet", cmd.uniKey)

	if ckey.status == cache_missing {
		cmd.reply(errcode.ERR_NOTFOUND, nil, -1)
		return
	} else {

		if ckey.status == cache_ok {
			v := ckey.values[cmd.cns.oldV.GetName()]
			if !v.Equal(cmd.cns.oldV) {
				cmd.reply(errcode.ERR_NOT_EQUAL, ckey.values, ckey.version)
				return
			}
		}

		ctx.commands = append(ctx.commands, cmd)
		ctx.fields = map[string]*proto.Field{}

		if ckey.status == cache_ok {
			ctx.writeBackFlag = write_back_update //数据存在执行update
			ctx.fields[cmd.cns.oldV.GetName()] = cmd.cns.newV
		}

		return
	}
}

func processCompareAndSetNx(ckey *cacheKey, cmd *command, ctx *cmdContext) {

	Debugln("processCompareAndSetNx", cmd.uniKey)

	if ckey.status == cache_ok {
		v := ckey.values[cmd.cns.oldV.GetName()]
		if !v.Equal(cmd.cns.oldV) {
			//存在但不相等
			cmd.reply(errcode.ERR_NOT_EQUAL, ckey.values, ckey.version)
			return
		}
	}

	ctx.commands = append(ctx.commands, cmd)
	ctx.fields = map[string]*proto.Field{}

	if ckey.status == cache_ok {
		ctx.writeBackFlag = write_back_update //数据存在执行update
		ctx.fields[cmd.cns.oldV.GetName()] = cmd.cns.newV
	} else if ckey.status == cache_missing {
		ctx.writeBackFlag = write_back_insert
		ctx.fields[cmd.cns.oldV.GetName()] = cmd.cns.newV
	}

	return
}

func processIncrBy(ckey *cacheKey, cmd *command, ctx *cmdContext) {

	Debugln("processIncrBy", cmd.uniKey)

	if nil != cmd.version {
		if ckey.status == cache_missing {
			cmd.reply(errcode.ERR_NOTFOUND, nil, -1)
			return
		}

		if ckey.status == cache_ok && *cmd.version != ckey.version {
			cmd.reply(errcode.ERR_VERSION, nil, ckey.version)
			return
		}
	}

	ctx.commands = append(ctx.commands, cmd)

	if ckey.status == cache_ok || ckey.status == cache_missing {

		if ckey.status == cache_ok {
			ctx.writeBackFlag = write_back_update //数据存在执行update
		} else if ckey.status == cache_missing {
			ctx.writeBackFlag = write_back_insert
		}
	} else {
		ctx.fields = map[string]*proto.Field{}
	}

	return
}

func processDecrBy(ckey *cacheKey, cmd *command, ctx *cmdContext) {

	Debugln("processDecrBy", cmd.uniKey)

	if nil != cmd.version {
		if ckey.status == cache_missing {
			cmd.reply(errcode.ERR_NOTFOUND, nil, -1)
			return
		}

		if ckey.status == cache_ok && *cmd.version != ckey.version {
			cmd.reply(errcode.ERR_VERSION, nil, ckey.version)
			return
		}
	}

	ctx.commands = append(ctx.commands, cmd)

	if ckey.status == cache_ok || ckey.status == cache_missing {

		if ckey.status == cache_ok {
			ctx.writeBackFlag = write_back_update //数据存在执行update
		} else if ckey.status == cache_missing {
			ctx.writeBackFlag = write_back_insert
		}
	} else {
		ctx.fields = map[string]*proto.Field{}
	}

	return
}

func processDel(ckey *cacheKey, cmd *command, ctx *cmdContext) {

	Debugln("processDel", cmd.uniKey)

	if ckey.status == cache_missing {
		cmd.reply(errcode.ERR_NOTFOUND, nil, -1)
		return
	} else {
		if nil != cmd.version && ckey.status == cache_ok && *cmd.version != ckey.version {
			cmd.reply(errcode.ERR_VERSION, nil, ckey.version)
			return
		}

		ctx.commands = append(ctx.commands, cmd)

		if ckey.status == cache_ok {
			ctx.writeBackFlag = write_back_delete
		} else {
			ctx.fields = map[string]*proto.Field{}
		}

		return
	}
}

/*
 *   处理ckey关联的命令
 *   fromClient = true:表示从网络层直接调用过来
 *   fromClient = false:表示前一条命令处理完毕后，由其它模块调用过来
 *
 *   连续的get命令可以被合并处理，其余命令排队处理。
 *   当前一条命令尚未执行完毕,队列被锁定，新到的命令插入队列，不处理。
 *
 *   当一条命令执行完毕后，调用processCmd,检查是否有排队的命令，如果有继续执行排队命令。
 *
 */

func processCmd(ckey *cacheKey, fromClient bool) {

	ckey.mtx.Lock()

	if !fromClient {
		//前一条命令执行完毕，解锁队列
		ckey.unlockCmdQueue()
	}

	if ckey.cmdQueueLocked || ckey.cmdQueue.Len() == 0 {
		ckey.mtx.Unlock()
		return
	}

	ctx := &cmdContext{
		commands: []*command{},
	}

	now := time.Now()

	config := conf.GetConfig()

	for ckey.cmdQueue.Len() > 0 {
		e := ckey.cmdQueue.Front()
		cmd := e.Value.(*command)
		if cmd.isClosed() {
			//客户端连接已经关闭
			ckey.cmdQueue.Remove(e)
			cmd.dontReply()
		} else if now.After(cmd.deadline) {
			ckey.cmdQueue.Remove(e)
			//已经超时
			cmd.dontReply()
		} else {
			if cmd.cmdType == cmdGet {
				ckey.cmdQueue.Remove(e)
				processGet(ckey, cmd, ctx)
			} else {
				if len(ctx.commands) > 0 {
					//前面已经有get命令了
					break
				} else {
					ckey.cmdQueue.Remove(e)
					switch cmd.cmdType {
					case cmdSet:
						processSet(ckey, cmd, ctx)
					case cmdSetNx:
						processSetNx(ckey, cmd, ctx)
					case cmdCompareAndSet:
						processCompareAndSet(ckey, cmd, ctx)
					case cmdCompareAndSetNx:
						processCompareAndSetNx(ckey, cmd, ctx)
					case cmdIncrBy:
						processIncrBy(ckey, cmd, ctx)
					case cmdDecrBy:
						processDecrBy(ckey, cmd, ctx)
					case cmdDel:
						processDel(ckey, cmd, ctx)
					default:
						//记录日志
					}
					//会产生数据变更的命令只能按序执行
					if len(ctx.commands) > 0 {
						break
					}
				}
			}
		}
	}

	if len(ctx.commands) == 0 {
		ckey.mtx.Unlock()
		return
	}

	if ckey.status == cache_new {
		if !pushSqlLoadReq(ctx, fromClient) {
			ckey.mtx.Unlock()
			if config.ReplyBusyOnQueueFull {
				ctx.reply(errcode.ERR_BUSY, nil, -1)
			} else {
				ctx.dontReply()
			}
			/*
			 * 只有在fromClient==true时pushSqlLoadReq才有可能返回false
			 * 此时必定是由网络层直接调用上来，不会存在排队未处理的cmd,所以不需要调用processCmd
			 */
			//processCmd(ckey, fromClient)
		} else {
			ckey.lockCmdQueue()
			ckey.mtx.Unlock()
		}
	} else {
		ckey.lockCmdQueue()
		ckey.mtx.Unlock()
		if ctx.getCmdType() == cmdGet {
			ckey.store.issueReadReq(ctx)
		} else {
			ckey.store.issueUpdate(ctx)
		}
	}
}
