package flyfish

import (
	"github.com/sniperHW/flyfish/errcode"
	//"github.com/sniperHW/flyfish/proto"
)

type sqlResponseI interface {
	onSqlResp(ctx *processContext, errno int32)
}

func onSqlExecError(ctx *processContext) {
	Debugln("onSqlExecError key", ctx.getUniKey())
	ctx.reply(errcode.ERR_SQLERROR, nil, -1)
	ctx.getCacheKey().processQueueCmd()
}

var sqlResponse sqlResponseI
