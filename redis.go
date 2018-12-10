package flyfish

import(
	"github.com/sniperHW/kendynet/util"
	"github.com/go-redis/redis"
	"sync"
	//"strconv"
	"fmt"
	//"strings"
	"flyfish/conf"
	//"flyfish/errcode"
	//protocol "flyfish/proto"
)


var redis_once sync.Once
var cli *redis.Client

var redisProcessQueue *util.BlockQueue

func pushRedis(ctx *processContext) {
	Debugln("pushRedis",ctx.getUniKey())
	redisProcessQueue.Add(ctx)
}

func pushRedisNoWait(ctx *processContext) {
	Debugln("pushRedisNoWait",ctx.getUniKey())
	redisProcessQueue.AddNoWait(ctx)	
}


func redisRoutine(queue *util.BlockQueue) {
	redisPipeliner_ := newRedisPipeliner(conf.RedisPipelineSize)
	for {
		closed, localList := queue.Get()	
		for _,v := range(localList) {
			ctx := v.(*processContext)
			redisPipeliner_.append(ctx)
		}
		redisPipeliner_.exec()
		if closed {
			return
		}
	}	
}

func RedisClose() {
	
}

func RedisInit(Addr string,Password string) bool {
	redis_once.Do(func() {
		cli = redis.NewClient(&redis.Options{
			Addr:     Addr,
			Password: Password,
		})

		if nil != cli {
			redisProcessQueue = util.NewBlockQueueWithName(fmt.Sprintf("redis"),conf.RedisEventQueueSize)
			for i := 0; i < conf.RedisProcessPoolSize; i++ {
				go redisRoutine(redisProcessQueue)
			}

			/*go func(){
				for {
					time.Sleep(time.Second)
					fmt.Println("---------------redisProcessQueue-------------")
					for _,v := range(redisProcessQueue) {
						fmt.Println(v.Len())
					}
				}
			}()*/		
		}
	})
	return cli != nil
}