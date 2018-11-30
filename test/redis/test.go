package main 


import (
	//"strconv"
	//"strings"
	//"sync"
	"github.com/go-redis/redis"
	//"time"
	"fmt"
	//"github.com/sniperHW/kendynet/event"
	//"github.com/sniperHW/kendynet/asyn"	
	//"math/rand"
	"reflect"
)


func main() {

	cli := redis.NewClient(&redis.Options{
		Addr:"localhost:6379",
	})

	fields := map[string]interface{}{}
	fields["key"] = 127.98
 	cli.HMSet("test",fields)

 	ret := cli.HMGet("test","key")
 	result,_ := ret.Result()
 	fmt.Println(result[0])

 	tt := reflect.TypeOf(result[0])
	name := tt.String()

	fmt.Println(name)


	//asyn.SetRoutinePool(asyn.NewRoutinePool(100))
/*	eventQueue := event.NewEventQueue()

	asynHMSet := asyn.AsynWrap(eventQueue,cli.HMSet)


	var callback func(ret []interface{})

	c := 0

	nextShow := time.Now().Unix()

	callback = func(ret []interface{}) {
		_ , err := ret[0].(*redis.StatusCmd).Result()
		if err != nil {
			fmt.Println(err)
		} else {
			c++
			now := time.Now().Unix()
			if now >= nextShow {
				fmt.Println(c,now)
				c = 0
				nextShow = now + 1
			}
		}
		key := fmt.Sprintf("%s:%s_%d","test","huangwei",rand.Int()%1000000)
		fields := map[string]interface{}{}
		fields["age"] = 36
		fields["phone"] = "18602174540"
		asynHMSet(callback,key,fields)
	}

	var i int
	for i = 0; i < 1000;i++ {
		key := fmt.Sprintf("%s:%s_%d","test","huangwei",rand.Int()%1000000)
		fields := map[string]interface{}{}
		fields["age"] = 36
		fields["phone"] = "18602174540"
		asynHMSet(callback,key,fields)		
	}

	eventQueue.Run()
*/	
}