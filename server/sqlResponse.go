package server

import (
	"github.com/sniperHW/flyfish/errcode"
)

func onSqlExecError(ctx *cmdContext) {
	Debugln("onSqlExecError key", ctx.getUniKey())
	ckey := ctx.getCacheKey()
	ctx.reply(errcode.ERR_SQLERROR, nil, -1)
	if !ckey.tryRemoveTmpKey(errcode.ERR_SQLERROR) {
		ckey.processQueueCmd()
	}
}

/*
 *  如果数据库中不存在，也需要在内存中建立kv,并将其标记为缺失以供后续访问
 */
func onSqlNotFound(ctx *cmdContext) {
	Debugln("onSqlNotFound key", ctx.getUniKey())
	cmdType := ctx.getCmdType()
	ckey := ctx.getCacheKey()
	ckey.setMissing()
	if cmdType == cmdDel || cmdType == cmdCompareAndSet || cmdType == cmdGet {
		ctx.errno = errcode.ERR_NOTFOUND
		ctx.fields = nil
		ctx.version = 0
		//向副本同步插入操作
		ckey.store.issueAddKv(ctx)
	} else {
		ctx.writeBackFlag = write_back_insert
		ckey.store.issueUpdate(ctx)
	}
}

func onSqlLoadOKGet(ctx *cmdContext) {
	Debugln("onSqlLoadOKGet")
	version := ctx.fields["__version__"].GetInt()
	ckey := ctx.getCacheKey()

	ckey.mtx.Lock()
	ckey.setValueNoLock(ctx)
	ckey.setOKNoLock(version)
	ckey.mtx.Unlock()

	ctx.errno = errcode.ERR_OK
	ctx.version = version
	ckey.store.issueAddKv(ctx)
}

func onSqlLoadOKSet(ctx *cmdContext) {

	Debugln("onSqlLoadOKSet")

	version := ctx.fields["__version__"].GetInt()

	Debugln("onSqlLoadOKSet", version)

	cmd := ctx.getCmd()
	ckey := ctx.getCacheKey()

	ckey.mtx.Lock()
	ckey.setValueNoLock(ctx)
	ckey.setOKNoLock(version)
	ckey.mtx.Unlock()

	cmdType := cmd.cmdType

	switch cmdType {
	case cmdSet:
		if nil != cmd.version && *cmd.version != version {
			//版本号不对
			ctx.errno = errcode.ERR_VERSION
			ctx.version = version
			ckey.store.issueAddKv(ctx)
			return
		} else {
			ctx.fields = ctx.getCmd().fields
		}
	case cmdCompareAndSet, cmdCompareAndSetNx:
		dbV := ctx.fields[cmd.cns.oldV.GetName()]
		if !dbV.Equal(cmd.cns.oldV) {
			ctx.errno = errcode.ERR_NOT_EQUAL
			ctx.version = version
			ckey.store.issueAddKv(ctx)
			return
		} else {
			ctx.fields[cmd.cns.oldV.GetName()] = cmd.cns.newV
		}
	case cmdSetNx:
		ctx.errno = errcode.ERR_KEY_EXIST
		ctx.version = version
		ckey.store.issueAddKv(ctx)
		return
	default:
	}
	ctx.writeBackFlag = write_back_update //sql中存在,使用update回写
	ckey.store.issueUpdate(ctx)
}

func onSqlLoadOKDel(ctx *cmdContext) {

	Debugln("onSqlLoadOKDel")

	version := ctx.fields["__version__"].GetInt()
	cmd := ctx.getCmd()
	ckey := ctx.getCacheKey()
	ckey.mtx.Lock()
	ckey.setValueNoLock(ctx)
	ckey.setOKNoLock(version)
	ckey.mtx.Unlock()
	if nil != cmd.version && *cmd.version != version {
		//版本号不对
		ctx.errno = errcode.ERR_VERSION
		ctx.version = version
		ckey.store.issueAddKv(ctx)
	} else {
		ctx.writeBackFlag = write_back_delete
		ckey.store.issueUpdate(ctx)
	}
}

func onSqlLoadOK(ctx *cmdContext) {
	Debugln("onSqlLoadOK")
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

func onSqlResp(ctx *cmdContext, errno int32) {
	Debugln("onSqlResp", ctx.getUniKey(), ctx.getCmdType(), errno)
	if errno == errcode.ERR_OK {
		onSqlLoadOK(ctx)
	} else if errno == errcode.ERR_NOTFOUND {
		onSqlNotFound(ctx)
	} else {
		onSqlExecError(ctx)
	}
}
