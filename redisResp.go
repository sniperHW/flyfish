package flyfish

import(
	"flyfish/errcode"
	"sync/atomic"
)

func processRedisResp(args []interface{}) {
	ctx := args[0].(*processContext)
	newVersion := args[1].(int64)
	ckey := ctx.getCacheKey()	
	if ctx.errno == errcode.ERR_STALE_CACHE {
		/*  redis中的数据与flyfish key不一致
		 *  将ckey重置为cache_new，强制从数据库取值刷新redis
		 */
		ckey.reset()			
		ctx.writeBackFlag = write_back_none //数据存在执行update
		ctx.redisFlag     = redis_none
		//到数据库加载
		pushSQLLoad(ctx)

	} else {
		ckey.unlock()
		
		if ctx.errno == errcode.ERR_OK {
			if ctx.redisFlag == redis_get || ctx.redisFlag == redis_set_only {
				ckey.setOK(newVersion)
			} else if ctx.redisFlag == redis_del {
				ckey.setMissing()
				//投递sql删除请求
				pushSQLWriteBack(ctx)	
			} else {
				ckey.setOK(newVersion)
				pushSQLWriteBack(ctx)
			} 
		}

		ckey.process()
	}

	atomic.AddInt32(&redisReqCount,-1)
}

func onRedisResp(ctx *processContext) {

	newVersion := int64(0)
	//先发出响应

	if ctx.errno != errcode.ERR_STALE_CACHE {
		newVersion = ctx.fields["__version__"].GetInt()
		ctx.reply(ctx.errno,ctx.fields,newVersion)
	}

	postKeyEventNoWait(ctx.getUniKey(),processRedisResp,ctx,newVersion)
}