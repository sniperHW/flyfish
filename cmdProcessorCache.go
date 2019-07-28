package flyfish

import (
	"flyfish/conf"
	"flyfish/errcode"
	"flyfish/proto"
	"sync/atomic"
	"time"
)

type cmdProcessorCache struct {
}

func (this *cmdProcessorCache) processGet(ckey *cacheKey, cmd *command) *processContext {
	Debugln("processGet", cmd.uniKey)
	if ckey.status == cache_missing {
		cmd.reply(errcode.ERR_NOTFOUND, nil, -1)
		return nil
	} else if ckey.status == cache_ok {
		cmd.reply(errcode.ERR_OK, ckey.values, ckey.version)
		return nil
	} else {

		ctx := &processContext{
			commands: []*command{cmd},
			fields:   map[string]*proto.Field{},
		}

		ctx.fields["__version__"] = proto.PackField("__version__", ckey.version)
		for _, v := range cmd.fields {
			ctx.fields[v.GetName()] = v
		}
		return ctx
	}
}

func (this *cmdProcessorCache) processSet(ckey *cacheKey, cmd *command) *processContext {
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

	ctx := &processContext{
		commands:    []*command{cmd},
		fields:      map[string]*proto.Field{},
		replyOnDbOk: cmd.replyOnDbOk,
	}
	if ckey.status == cache_ok {
		ckey.setOKNoLock(ckey.version + 1)
		ctx.writeBackFlag = write_back_update //数据存在执行update
		ctx.fields["__version__"] = proto.PackField("__version__", ckey.version)
		for _, v := range cmd.fields {
			ckey.values[v.GetName()] = v
			ctx.fields[v.GetName()] = v
		}
		ctx.reply(errcode.ERR_OK, ctx.fields, ckey.version)
	} else if ckey.status == cache_missing {
		ckey.setDefaultValue(ctx)
		ckey.setOKNoLock(1)
		ctx.writeBackFlag = write_back_insert //数据不存在执行insert
		ctx.fields["__version__"] = proto.PackField("__version__", ckey.version)
		for _, v := range cmd.fields {
			ckey.values[v.GetName()] = v
			ctx.fields[v.GetName()] = v
		}
		ctx.reply(errcode.ERR_OK, ctx.fields, ckey.version)
	}

	return ctx
}

func (this *cmdProcessorCache) processSetNx(ckey *cacheKey, cmd *command) *processContext {
	Debugln("processSetNx", cmd.uniKey)
	if ckey.status == cache_ok {
		//记录已经存在，不能再设置
		cmd.reply(errcode.ERR_KEY_EXIST, nil, ckey.version)
		return nil
	}

	ctx := &processContext{
		commands:    []*command{cmd},
		fields:      map[string]*proto.Field{},
		replyOnDbOk: cmd.replyOnDbOk,
	}

	if ckey.status == cache_missing {
		ckey.setDefaultValue(ctx)
		ckey.setOKNoLock(1)
		ctx.fields["__version__"] = proto.PackField("__version__", ckey.version)
		for _, v := range cmd.fields {
			ckey.values[v.GetName()] = v
			ctx.fields[v.GetName()] = v
		}
		ctx.writeBackFlag = write_back_insert //数据不存在执行insert

		ctx.reply(errcode.ERR_OK, ctx.fields, ckey.version)

	} else {
		for _, v := range cmd.fields {
			ctx.fields[v.GetName()] = v
		}
	}

	return ctx
}

func (this *cmdProcessorCache) processCompareAndSet(ckey *cacheKey, cmd *command) *processContext {

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

		ctx := &processContext{
			commands:    []*command{cmd},
			fields:      map[string]*proto.Field{},
			replyOnDbOk: cmd.replyOnDbOk,
		}

		if ckey.status == cache_ok {
			ckey.setOKNoLock(ckey.version + 1)
			ctx.writeBackFlag = write_back_update //数据存在执行update
			ctx.fields["__version__"] = proto.PackField("__version__", ckey.version)
			ckey.values[cmd.cns.oldV.GetName()] = cmd.cns.newV
			ctx.fields[cmd.cns.oldV.GetName()] = cmd.cns.newV
			ctx.reply(errcode.ERR_OK, ctx.fields, ckey.version)
		}

		return ctx
	}
}

func (this *cmdProcessorCache) processCompareAndSetNx(ckey *cacheKey, cmd *command) *processContext {

	Debugln("processCompareAndSetNx", cmd.uniKey)

	if ckey.status == cache_ok {
		v := ckey.values[cmd.cns.oldV.GetName()]
		if !v.Equal(cmd.cns.oldV) {
			//存在但不相等
			cmd.reply(errcode.ERR_NOT_EQUAL, ckey.values, ckey.version)
			return nil
		}
	}

	ctx := &processContext{
		commands:    []*command{cmd},
		fields:      map[string]*proto.Field{},
		replyOnDbOk: cmd.replyOnDbOk,
	}

	if ckey.status == cache_ok {
		ckey.setOKNoLock(ckey.version + 1)
		ctx.writeBackFlag = write_back_update //数据存在执行update
		ctx.fields["__version__"] = proto.PackField("__version__", ckey.version)
		ckey.values[cmd.cns.oldV.GetName()] = cmd.cns.newV
		ctx.fields[cmd.cns.oldV.GetName()] = cmd.cns.newV
		ctx.reply(errcode.ERR_OK, ctx.fields, ckey.version)
	} else if ckey.status == cache_missing {
		ckey.setDefaultValue(ctx)
		ckey.setOKNoLock(1)
		ctx.writeBackFlag = write_back_insert
		ctx.fields["__version__"] = proto.PackField("__version__", ckey.version)
		ckey.values[cmd.cns.oldV.GetName()] = cmd.cns.newV
		ctx.fields[cmd.cns.oldV.GetName()] = cmd.cns.newV
		ctx.reply(errcode.ERR_OK, ctx.fields, ckey.version)
	}

	return ctx
}

func (this *cmdProcessorCache) processIncrBy(ckey *cacheKey, cmd *command) *processContext {

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

	ctx := &processContext{
		commands:    []*command{cmd},
		fields:      map[string]*proto.Field{},
		replyOnDbOk: cmd.replyOnDbOk,
	}

	if ckey.status == cache_ok || ckey.status == cache_missing {

		if ckey.status == cache_ok {
			ckey.setOKNoLock(ckey.version + 1)
			ctx.writeBackFlag = write_back_update //数据存在执行update
		} else if ckey.status == cache_missing {
			ckey.setDefaultValue(ctx)
			ckey.setOKNoLock(1)
			ctx.writeBackFlag = write_back_insert
		}
		oldV := ckey.values[cmd.incrDecr.GetName()]
		newV := proto.PackField(cmd.incrDecr.GetName(), oldV.GetInt()+cmd.incrDecr.GetInt())
		ctx.fields[cmd.incrDecr.GetName()] = newV
		ckey.values[cmd.incrDecr.GetName()] = newV
		ctx.fields["__version__"] = proto.PackField("__version__", ckey.version)
		ctx.reply(errcode.ERR_OK, ctx.fields, ckey.version)
	}

	return ctx
}

func (this *cmdProcessorCache) processDecrBy(ckey *cacheKey, cmd *command) *processContext {

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

	ctx := &processContext{
		commands:    []*command{cmd},
		fields:      map[string]*proto.Field{},
		replyOnDbOk: cmd.replyOnDbOk,
	}

	if ckey.status == cache_ok || ckey.status == cache_missing {

		if ckey.status == cache_ok {
			ckey.setOKNoLock(ckey.version + 1)
			ctx.writeBackFlag = write_back_update //数据存在执行update
		} else if ckey.status == cache_missing {
			ctx.writeBackFlag = write_back_insert
			ckey.setDefaultValue(ctx)
			ckey.setOKNoLock(1)
		}
		oldV := ckey.values[cmd.incrDecr.GetName()]
		newV := proto.PackField(cmd.incrDecr.GetName(), oldV.GetInt()-cmd.incrDecr.GetInt())
		ctx.fields[cmd.incrDecr.GetName()] = newV
		ckey.values[cmd.incrDecr.GetName()] = newV
		ctx.fields["__version__"] = proto.PackField("__version__", ckey.version)
		ctx.reply(errcode.ERR_OK, ctx.fields, ckey.version)
	}

	return ctx
}

func (this *cmdProcessorCache) processDel(ckey *cacheKey, cmd *command) *processContext {

	Debugln("processDel", cmd.uniKey)

	if ckey.status == cache_missing {
		cmd.reply(errcode.ERR_NOTFOUND, nil, -1)
		return nil
	} else {
		if nil != cmd.version && ckey.status == cache_ok && *cmd.version != ckey.version {
			cmd.reply(errcode.ERR_VERSION, nil, ckey.version)
			return nil
		}

		ctx := &processContext{
			commands:    []*command{cmd},
			fields:      map[string]*proto.Field{},
			replyOnDbOk: cmd.replyOnDbOk,
		}

		if ckey.status == cache_ok {
			ckey.setMissingNoLock()
			ctx.writeBackFlag = write_back_delete
			ctx.reply(errcode.ERR_OK, ctx.fields, ckey.version)
		}

		return ctx
	}
}

func (this *cmdProcessorCache) processCmd(ckey *cacheKey, fromClient bool) {

	ckey.mtx.Lock()

	if !fromClient {
		ckey.unlockCmdQueue()
	}

	if ckey.cmdQueueLocked || ckey.cmdQueue.Len() == 0 {
		ckey.mtx.Unlock()
		return
	}

	var ctx *processContext

	now := time.Now()

	for ckey.cmdQueue.Len() > 0 {
		e := ckey.cmdQueue.Front()
		cmd := e.Value.(*command)
		ckey.cmdQueue.Remove(e)
		//Debugln("process", cmd.cmdType)
		if now.After(cmd.deadline) {
			//已经超时
			atomic.AddInt32(&cmdCount, -1)
		} else {

			if fromClient && causeWriteBackCmd(cmd.cmdType) && ckey.unit.updateQueueFull() {
				if conf.GetConfig().ReplyBusyOnQueueFull {
					ctx.reply(errcode.ERR_BUSY, nil, -1)
				} else {
					atomic.AddInt32(&cmdCount, -1)
				}
			} else {

				switch cmd.cmdType {
				case cmdGet:
					ctx = this.processGet(ckey, cmd)
					break
				case cmdSet:
					ctx = this.processSet(ckey, cmd)
					break
				case cmdSetNx:
					ctx = this.processSetNx(ckey, cmd)
					break
				case cmdCompareAndSet:
					ctx = this.processCompareAndSet(ckey, cmd)
					break
				case cmdCompareAndSetNx:
					ctx = this.processCompareAndSetNx(ckey, cmd)
					break
				case cmdIncrBy:
					ctx = this.processIncrBy(ckey, cmd)
					break
				case cmdDecrBy:
					ctx = this.processDecrBy(ckey, cmd)
					break
				case cmdDel:
					ctx = this.processDel(ckey, cmd)
					break
				default:
					//记录日志
					break
				}

				if nil != ctx {
					break
				}
			}
		}
	}

	if nil == ctx {
		ckey.mtx.Unlock()
		return
	}

	if ckey.status == cache_new {
		fullReturn := fromClient
		if !ckey.unit.pushSqlLoadReq(ctx, fullReturn) {
			ckey.mtx.Unlock()
			if conf.GetConfig().ReplyBusyOnQueueFull {
				ctx.reply(errcode.ERR_BUSY, nil, -1)
			} else {
				atomic.AddInt32(&cmdCount, -1)
			}
			ckey.process_(fromClient)
			return
		} else {
			ckey.lockCmdQueue()
			ckey.mtx.Unlock()
		}
	} else {
		if ctx.replyOnDbOk {
			ckey.lockCmdQueue()
		}
		ckey.mtx.Unlock()
		ckey.unit.pushSqlWriteBackReq(ctx)
	}
}
