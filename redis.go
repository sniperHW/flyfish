package flyfish

import (
	"flyfish/conf"
	"fmt"
	"github.com/go-redis/redis"
	"github.com/sniperHW/kendynet/util"
	//"sync"
	//"sync/atomic"
)

var (
	cli               *redis.Client
	redisReqCount     int32
	redisProcessQueue *util.BlockQueue
)

func redisRoutine(queue *util.BlockQueue) {
	redisPipeliner_ := newRedisPipeliner(conf.DefConfig.RedisPipelineSize)
	for {
		closed, localList := queue.Get()
		for _, v := range localList {
			ctx := v.(*processContext)
			redisPipeliner_.append(ctx)
		}
		redisPipeliner_.exec()
		if closed {
			return
		}
	}
}

func RedisInit(host string, port int, Password string) bool {
	cli = redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%d", host, port),
		Password: Password,
	})
	if nil != cli {
		redisProcessQueue = util.NewBlockQueueWithName(fmt.Sprintf("redis"), conf.DefConfig.RedisEventQueueSize)
		for i := 0; i < conf.DefConfig.RedisProcessPoolSize; i++ {
			go redisRoutine(redisProcessQueue)
		}
	}
	return cli != nil
}
