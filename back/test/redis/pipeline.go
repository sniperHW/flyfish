package main

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/go-redis/redis"
)

func main() {

	cli := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	setStr := `
		local v = redis.call('hget',KEYS[1],ARGV[1])
		if not v then
			redis.call('hmset',KEYS[1],ARGV[1],ARGV[2])
			return ARGV[2]
		else
			return ARGV[2]
		end
	`

	c := 0

	nextShow := time.Now().Unix()

	for i := 0; i < 1; i++ {

		go func() {

			for {
				pipe := cli.Pipeline()

				rets := []interface{}{}

				var i int
				for i = 0; i < 100; i++ {
					keys := []string{fmt.Sprintf("%s:%s_%d", "test", "huangwei", rand.Int()%1000000)}
					cmd := pipe.Eval(setStr, keys, "__version__", i)
					/*key := fmt.Sprintf("%s:%s_%d","test","huangwei",rand.Int()%1000000)
					fields := map[string]interface{}{}
					fields["age"] = 33
					cmd := pipe.HMSet(key,fields)*/
					rets = append(rets, cmd)
				}

				_, err := pipe.Exec()

				if nil == err {
					/*for _,v := range(rets) {
						result,err1 := v.(*redis.Cmd).Result()
						fmt.Println(result.(string),err1)
					}*/
					c = c + 100
					now := time.Now().Unix()
					if now >= nextShow {
						fmt.Println(c, now)
						c = 0
						nextShow = now + 1
					}
				} else {
					fmt.Println(err)
				}
			}
		}()
	}

	sigStop := make(chan bool)
	_, _ = <-sigStop
}
