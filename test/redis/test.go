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
		Addr: "localhost:6379",
	})

	keys := []string{"users1:huangwei:1"}
	args := []interface{}{}
	args = append(args, "age", 41, 42, "__version__", 1537)

	const strCompareAndSet string = `
	local v = redis.call('hmget',KEYS[1],ARGV[4],ARGV[1])
	if (not v) or (not v[1]) or (not v[2]) then
		return version
	else

		if tonumber(v[1]) ~= ARGV[5] - 1 then
			return "err_version" .. v[1]
		end

		if 'number' == type(ARGV[2]) then
			v[2] = tonumber(v)
		end

		if v[2] ~= ARGV[2] then
			return v[2]
		else
			redis.call('hmset',KEYS[1],ARGV[1],ARGV[3],ARGV[4],ARGV[5])
			return ARGV[3]
		end
	end
`

	cmd := cli.Eval(strCompareAndSet, keys, args...)

	r, err1 := cmd.Result()

	if nil != err1 {
		fmt.Println(err1)
	} else {
		fmt.Println(r, reflect.TypeOf(r).String(), err1)
	}
	/*fields := map[string]interface{}{}
		fields["key"] = 127.98
	 	cli.HMSet("test",fields)

	 	ret := cli.HMGet("test","key")
	 	result,_ := ret.Result()
	 	fmt.Println(result[0])

	 	tt := reflect.TypeOf(result[0])
		name := tt.String()

		fmt.Println(name)*/

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
