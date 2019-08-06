package flyfish

import (
	"github.com/sniperHW/flyfish/errcode"
	"sync/atomic"
)

func onRedisResp(ctx *processContext) {

	Debugln("onRedisResp key:", ctx.getCmdType(), ctx.getUniKey(), ctx.errno)

	ckey := ctx.getCacheKey()
	if ctx.errno == errcode.ERR_STALE_CACHE {
		/*  redis中的数据与flyfish key不一致
		 *  将ckey重置为cache_new，强制从数据库取值刷新redis
		 */
		ckey.unit.onRedisStale(ckey, ctx)
	} else {
		if ctx.errno == errcode.ERR_OK {
			if ctx.redisFlag == redis_get || ctx.redisFlag == redis_set_only {
				newVersion := ctx.fields["__version__"].GetInt()
				ckey.setOK(newVersion)
				ctx.reply(ctx.errno, ctx.fields, newVersion)
				ckey.processQueueCmd()
			} else {
				if ctx.redisFlag == redis_del {
					ckey.setMissing()
				} else {
					ckey.setOK(ctx.fields["__version__"].GetInt())
				}
				ckey.unit.doWriteBack(ctx)
			}
		}
	}

	atomic.AddInt32(&redisReqCount, -1)
}
