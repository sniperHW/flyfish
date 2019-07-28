package flyfish

import (
	"flyfish/errcode"
	//"flyfish/proto"
)

type sqlResponseI interface {
	onSqlWriteBackResp(ctx *processContext, errno int32)
	onSqlResp(ctx *processContext, errno int32)
}

func onSqlExecError(ctx *processContext) {
	Debugln("onSqlExecError key", ctx.getUniKey())
	ctx.reply(errcode.ERR_SQLERROR, nil, -1)
	ctx.getCacheKey().processQueueCmd()
}

var sqlResponse sqlResponseI
