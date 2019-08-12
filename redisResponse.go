package flyfish

import (
	"container/list"
	"github.com/sniperHW/flyfish/errcode"
	"sync/atomic"
)

func onRedisStale(ckey *cacheKey, ctx *processContext) {
	/*  redis中的数据与flyfish key不一致
	 *  将ckey重置为cache_new，强制从数据库取值刷新redis
	 */
	ckey.mtx.Lock()
	defer ckey.mtx.Unlock()
	ckey.status = cache_new
	if ckey.writeBacked {
		ckey.cmdQueueLocked = false
		//尚未处理的cmd以及ctx中包含的cmd都不做响应，所以需要扣除正确的cmdCount
		atomic.AddInt32(&cmdCount, -int32(ckey.cmdQueue.Len()+len(ctx.commands)))
		ckey.cmdQueue = list.New()
	} else {
		ctx.writeBackFlag = write_back_none //数据存在执行update
		ctx.redisFlag = redis_none
		pushSqlLoadReq(ctx, false)
	}
}

func onRedisResp(ctx *processContext) {

	Debugln("onRedisResp key:", ctx.getCmdType(), ctx.getUniKey(), ctx.errno)

	ckey := ctx.getCacheKey()
	if ctx.errno == errcode.ERR_STALE_CACHE {
		/*  redis中的数据与flyfish key不一致
		 *  将ckey重置为cache_new，强制从数据库取值刷新redis
		 */
		onRedisStale(ckey, ctx)
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
