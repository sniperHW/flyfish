package server

import (
	"github.com/sniperHW/flyfish/conf"
	"github.com/sniperHW/flyfish/errcode"
	"github.com/sniperHW/flyfish/proto"
	"sync/atomic"
	"time"
)

func processGet(ckey *cacheKey, cmd *command) *cmdContext {
	Debugln("processGet", cmd.uniKey)
	ctx := &cmdContext{
		command: cmd,
		fields:  map[string]*proto.Field{},
	}
	return ctx
	/*if ckey.status == cache_missing {
		cmd.reply(errcode.ERR_NOTFOUND, nil, -1)
		return nil
	} else if ckey.status == cache_ok {
		cmd.reply(errcode.ERR_OK, ckey.values, ckey.version)
		return nil
	} else {

		ctx := &cmdContext{
			command: cmd,
			fields:  map[string]*proto.Field{},
		}
		return ctx
	}*/
}

func processSet(ckey *cacheKey, cmd *command) *cmdContext {
	Debugln("processSet", cmd.uniKey)
	if nil != cmd.version {
		if ckey.status == cache_missing {
			cmd.reply(errcode.ERR_NOTFOUND, nil, -1)
			return nil
		}

		if ckey.status == cache_ok && *cmd.version != ckey.version {
			cmd.reply(errcode.ERR_VERSION, nil, ckey.version)
			return nil
		}
	}

	ctx := &cmdContext{
		command: cmd,
	}

	if ckey.status == cache_ok {
		ctx.writeBackFlag = write_back_update //数据存在执行update
		ctx.fields = cmd.fields
	} else if ckey.status == cache_missing {
		ctx.writeBackFlag = write_back_insert //数据不存在执行insert
		ctx.fields = cmd.fields
	} else {
		ctx.fields = map[string]*proto.Field{}
	}

	return ctx
}

func processSetNx(ckey *cacheKey, cmd *command) *cmdContext {
	Debugln("processSetNx", cmd.uniKey)
	if ckey.status == cache_ok {
		//记录已经存在，不能再设置
		cmd.reply(errcode.ERR_KEY_EXIST, nil, ckey.version)
		return nil
	}

	ctx := &cmdContext{
		command: cmd,
	}

	if ckey.status == cache_missing {
		ctx.writeBackFlag = write_back_insert //数据不存在执行insert
		ctx.fields = cmd.fields
	} else {
		ctx.fields = map[string]*proto.Field{}
	}

	return ctx
}

func processCompareAndSet(ckey *cacheKey, cmd *command) *cmdContext {

	Debugln("processCompareAndSet", cmd.uniKey)

	if ckey.status == cache_missing {
		cmd.reply(errcode.ERR_NOTFOUND, nil, -1)
		return nil
	} else {

		if ckey.status == cache_ok {
			v := ckey.values[cmd.cns.oldV.GetName()]
			if !v.Equal(cmd.cns.oldV) {
				cmd.reply(errcode.ERR_NOT_EQUAL, ckey.values, ckey.version)
				return nil
			}
		}

		ctx := &cmdContext{
			command: cmd,
			fields:  map[string]*proto.Field{},
		}

		if ckey.status == cache_ok {
			ctx.writeBackFlag = write_back_update //数据存在执行update
			ctx.fields[cmd.cns.oldV.GetName()] = cmd.cns.newV
		}

		return ctx
	}
}

func processCompareAndSetNx(ckey *cacheKey, cmd *command) *cmdContext {

	Debugln("processCompareAndSetNx", cmd.uniKey)

	if ckey.status == cache_ok {
		v := ckey.values[cmd.cns.oldV.GetName()]
		if !v.Equal(cmd.cns.oldV) {
			//存在但不相等
			cmd.reply(errcode.ERR_NOT_EQUAL, ckey.values, ckey.version)
			return nil
		}
	}

	ctx := &cmdContext{
		command: cmd,
		fields:  map[string]*proto.Field{},
	}

	if ckey.status == cache_ok {
		ctx.writeBackFlag = write_back_update //数据存在执行update
		ctx.fields[cmd.cns.oldV.GetName()] = cmd.cns.newV
	} else if ckey.status == cache_missing {
		ctx.writeBackFlag = write_back_insert
		ctx.fields[cmd.cns.oldV.GetName()] = cmd.cns.newV
	}

	return ctx
}

func processIncrBy(ckey *cacheKey, cmd *command) *cmdContext {

	Debugln("processIncrBy", cmd.uniKey)

	if nil != cmd.version {
		if ckey.status == cache_missing {
			cmd.reply(errcode.ERR_NOTFOUND, nil, -1)
			return nil
		}

		if ckey.status == cache_ok && *cmd.version != ckey.version {
			cmd.reply(errcode.ERR_VERSION, nil, ckey.version)
			return nil
		}
	}

	ctx := &cmdContext{
		command: cmd,
	}

	if ckey.status == cache_ok || ckey.status == cache_missing {

		if ckey.status == cache_ok {
			ctx.writeBackFlag = write_back_update //数据存在执行update
		} else if ckey.status == cache_missing {
			ctx.writeBackFlag = write_back_insert
		}
	} else {
		ctx.fields = map[string]*proto.Field{}
	}

	return ctx
}

func processDecrBy(ckey *cacheKey, cmd *command) *cmdContext {

	Debugln("processDecrBy", cmd.uniKey)

	if nil != cmd.version {
		if ckey.status == cache_missing {
			cmd.reply(errcode.ERR_NOTFOUND, nil, -1)
			return nil
		}

		if ckey.status == cache_ok && *cmd.version != ckey.version {
			cmd.reply(errcode.ERR_VERSION, nil, ckey.version)
			return nil
		}
	}

	ctx := &cmdContext{
		command: cmd,
	}

	if ckey.status == cache_ok || ckey.status == cache_missing {

		if ckey.status == cache_ok {
			ctx.writeBackFlag = write_back_update //数据存在执行update
		} else if ckey.status == cache_missing {
			ctx.writeBackFlag = write_back_insert
		}
	} else {
		ctx.fields = map[string]*proto.Field{}
	}

	return ctx
}

func processDel(ckey *cacheKey, cmd *command) *cmdContext {

	Debugln("processDel", cmd.uniKey)

	if ckey.status == cache_missing {
		cmd.reply(errcode.ERR_NOTFOUND, nil, -1)
		return nil
	} else {
		if nil != cmd.version && ckey.status == cache_ok && *cmd.version != ckey.version {
			cmd.reply(errcode.ERR_VERSION, nil, ckey.version)
			return nil
		}

		ctx := &cmdContext{
			command: cmd,
		}

		if ckey.status == cache_ok {
			ctx.writeBackFlag = write_back_delete
		} else {
			ctx.fields = map[string]*proto.Field{}
		}

		return ctx
	}
}

func processCmd(ckey *cacheKey, fromClient bool) {

	//Infoln(ckey.values)

	ckey.mtx.Lock()

	if !fromClient {
		ckey.unlockCmdQueue()
	}

	if ckey.cmdQueueLocked || ckey.cmdQueue.Len() == 0 {
		ckey.mtx.Unlock()
		return
	}

	var ctx *cmdContext

	now := time.Now()

	config := conf.GetConfig()

	for ckey.cmdQueue.Len() > 0 {
		e := ckey.cmdQueue.Front()
		cmd := e.Value.(*command)
		ckey.cmdQueue.Remove(e)
		if now.After(cmd.deadline) {
			//已经超时
			atomic.AddInt64(&cmdCount, -1)
		} else {
			switch cmd.cmdType {
			case cmdGet:
				ctx = processGet(ckey, cmd)
			case cmdSet:
				ctx = processSet(ckey, cmd)
			case cmdSetNx:
				ctx = processSetNx(ckey, cmd)
			case cmdCompareAndSet:
				ctx = processCompareAndSet(ckey, cmd)
			case cmdCompareAndSetNx:
				ctx = processCompareAndSetNx(ckey, cmd)
			case cmdIncrBy:
				ctx = processIncrBy(ckey, cmd)
			case cmdDecrBy:
				ctx = processDecrBy(ckey, cmd)
			case cmdDel:
				ctx = processDel(ckey, cmd)
			default:
				//记录日志
			}

			if nil != ctx {
				break
			}
		}
	}

	if nil == ctx {
		ckey.mtx.Unlock()
		return
	}

	if ckey.status == cache_new {
		fullReturn := fromClient
		if !pushSqlLoadReq(ctx, fullReturn) {
			ckey.mtx.Unlock()
			if config.ReplyBusyOnQueueFull {
				ctx.reply(errcode.ERR_BUSY, nil, -1)
			} else {
				atomic.AddInt64(&cmdCount, -1)
			}
			processCmd(ckey, fromClient)
			return
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
