package flyfish

import (
	"github.com/sniperHW/flyfish/errcode"
	"github.com/sniperHW/flyfish/proto"
)

type sqlResponseLocalCache struct {
}

func (this sqlResponseLocalCache) onSqlNotFound(ctx *processContext) {
	Debugln("onSqlNotFound key", ctx.getUniKey())
	cmdType := ctx.getCmdType()
	ckey := ctx.getCacheKey()
	if cmdType == cmdGet || cmdType == cmdDel || cmdType == cmdCompareAndSet {
		ctx.reply(errcode.ERR_NOTFOUND, nil, -1)
		ckey.setMissing()
		ckey.processQueueCmd()
	} else {

		ckey.mtx.Lock()

		ckey.setDefaultValue(ctx)

		ckey.setOKNoLock(1)

		cmd := ctx.getCmd()

		if cmdType == cmdCompareAndSetNx {
			ctx.fields[cmd.cns.newV.GetName()] = cmd.cns.newV
			ckey.values[cmd.cns.newV.GetName()] = cmd.cns.newV
		} else if cmdType == cmdIncrBy {
			oldV := ckey.values[cmd.incrDecr.GetName()]
			newV := proto.PackField(cmd.incrDecr.GetName(), oldV.GetInt()+cmd.incrDecr.GetInt())
			ctx.fields[cmd.incrDecr.GetName()] = newV
			ckey.values[cmd.incrDecr.GetName()] = newV
		} else if cmdType == cmdDecrBy {
			oldV := ckey.values[cmd.incrDecr.GetName()]
			newV := proto.PackField(cmd.incrDecr.GetName(), oldV.GetInt()-cmd.incrDecr.GetInt())
			ctx.fields[cmd.incrDecr.GetName()] = newV
			ckey.values[cmd.incrDecr.GetName()] = newV
		} else {
			for _, v := range cmd.fields {
				ckey.values[v.GetName()] = v
			}
		}

		ckey.mtx.Unlock()

		ctx.fields["__version__"] = proto.PackField("__version__", 1)

		ctx.writeBackFlag = write_back_insert

		if !ctx.replyOnDbOk {
			ctx.reply(errcode.ERR_OK, nil, 1)
			ckey.processQueueCmd()
		}

		ckey.unit.pushSqlWriteBackReq(ctx)

	}
}

func (this sqlResponseLocalCache) onSqlLoadOKGet(ctx *processContext) {

	Debugln("onSqlLoadOKGet")

	version := ctx.fields["__version__"].GetInt()
	ckey := ctx.getCacheKey()
	ckey.mtx.Lock()
	ckey.setValue(ctx)
	ckey.setOKNoLock(version)
	ckey.mtx.Unlock()
	ctx.reply(errcode.ERR_OK, ctx.fields, version)
	ckey.processQueueCmd()
}

func (this sqlResponseLocalCache) onSqlLoadOKSet(ctx *processContext) {

	Debugln("onSqlLoadOKSet")

	version := ctx.fields["__version__"].GetInt()
	cmd := ctx.getCmd()
	ckey := ctx.getCacheKey()
	cmdType := cmd.cmdType
	ctx.writeBackFlag = write_back_none
	if cmdType == cmdSet {
		if nil != cmd.version && *cmd.version != version {
			//版本号不对
			ctx.reply(errcode.ERR_VERSION, nil, version)
		} else {
			//变更需要将版本号+1
			for _, v := range cmd.fields {
				ctx.fields[v.GetName()] = v
			}
			version++
			ctx.fields["__version__"] = proto.PackField("__version__", version)
			ctx.writeBackFlag = write_back_update //sql中存在,使用update回写
		}
	} else if cmdType == cmdCompareAndSet || cmdType == cmdCompareAndSetNx {
		dbV := ctx.fields[cmd.cns.oldV.GetName()]
		if !dbV.Equal(cmd.cns.oldV) {
			ctx.reply(errcode.ERR_NOT_EQUAL, ctx.fields, version)
		} else {
			version++
			ctx.fields["__version__"] = proto.PackField("__version__", version)
			ctx.fields[cmd.cns.oldV.GetName()] = cmd.cns.newV
			ctx.writeBackFlag = write_back_update //sql中存在,使用update回写
		}
	} else if cmdType == cmdSetNx {
		ctx.reply(errcode.ERR_KEY_EXIST, nil, version)
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
		version++
		ctx.fields["__version__"] = proto.PackField("__version__", version)
		ctx.writeBackFlag = write_back_update //sql中存在,使用update回写
	}

	ckey.mtx.Lock()

	ckey.setValue(ctx)

	ckey.setOKNoLock(version)

	ckey.mtx.Unlock()

	if ctx.writeBackFlag != write_back_none {
		if !ctx.replyOnDbOk {
			ctx.reply(errcode.ERR_OK, nil, version)
		}

		ckey.unit.pushSqlWriteBackReq(ctx)

		ckey.processQueueCmd()
	}
}

func (this sqlResponseLocalCache) onSqlLoadOKDel(ctx *processContext) {

	Debugln("onSqlLoadOKDel")

	var errCode int32
	version := ctx.fields["__version__"].GetInt()
	cmd := ctx.getCmd()
	ckey := ctx.getCacheKey()

	if nil != cmd.version && *cmd.version != version {
		//版本号不对
		errCode = errcode.ERR_VERSION
	} else {
		ctx.writeBackFlag = write_back_delete
		errCode = errcode.ERR_OK
	}

	ctx.reply(errCode, nil, version)

	if errCode == errcode.ERR_OK {
		ckey.setMissing()
		ckey.unit.pushSqlWriteBackReq(ctx)
	} else {
		ckey.mtx.Lock()
		ckey.setValue(ctx)
		ckey.setOKNoLock(version)
		ckey.mtx.Unlock()
	}

	if ctx.writeBackFlag == write_back_none || !ctx.replyOnDbOk {
		ckey.processQueueCmd()
	}
}

func (this sqlResponseLocalCache) onSqlLoadOK(ctx *processContext) {
	Debugln("onSqlLoadOK")
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

func (this sqlResponseLocalCache) onSqlWriteBackResp(ctx *processContext, errno int32) {
	Debugln("onSqlWriteBackResp", ctx.getUniKey(), ctx.getCmdType(), errno)
	ckey := ctx.getCacheKey()
	if errno == errcode.ERR_OK {
		version := ctx.fields["__version__"].GetInt()
		ctx.reply(errno, nil, version)
	} else {
		ctx.reply(errno, nil, -1)
		ckey.setMissing()
	}
	ckey.processQueueCmd()
}

func (this sqlResponseLocalCache) onSqlResp(ctx *processContext, errno int32) {
	Debugln("onSqlResp", ctx.getUniKey(), ctx.getCmdType(), errno)
	if errno == errcode.ERR_OK {
		this.onSqlLoadOK(ctx)
	} else if errno == errcode.ERR_NOTFOUND {
		this.onSqlNotFound(ctx)
	} else {
		onSqlExecError(ctx)
	}
}
