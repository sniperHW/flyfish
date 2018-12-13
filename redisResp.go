package flyfish

import(
	"flyfish/errcode"
)

func onRedisResp(ctx *processContext) {

	newVersion := ctx.fields["__version__"].GetInt()
	//先发出响应
	ctx.reply(ctx.errno,ctx.fields,newVersion)

	mainQueue.PostNoWait(func(){
		ckey := ctx.getCacheKey()	
		ckey.unlock()
		if ctx.errno == errcode.ERR_OK {
			if ctx.redisFlag == redis_get || ctx.redisFlag == redis_set_only {
				ckey.setOK(newVersion)
				processContextPut(ctx)
			} else if ctx.redisFlag == redis_del {
				ckey.setMissing()
				//投递sql删除请求
				pushSQLWriteBack(ctx)	
			} else {
				ckey.setOK(newVersion)
				pushSQLWriteBack(ctx)
			} 
		} else if ctx.errno == errcode.ERR_STALE_CACHE {
			/*  redis中的数据与flyfish key不一致
			 *  将ckey重置为cache_new，强制从数据库取值刷新redis
			*/
			ckey.reset()
			processContextPut(ctx)
		} else {
			processContextPut(ctx)
		}
		ckey.process()
	})
	
}