package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	kclient "github.com/sniperHW/flyfish/client"
	"github.com/sniperHW/flyfish/errcode"
	"github.com/sniperHW/flyfish/logger"
	"github.com/sniperHW/flyfish/server/kvnode"
)

var (
	getCount    int32
	getAvaDelay time.Duration
	id          int64
	keyrange    int64
)

func Get(c *kclient.Client) {

	nextID := atomic.AddInt64(&id, 1)

	key := fmt.Sprintf("%s:%d", "huangwei", nextID%keyrange)

	get := c.Get("users1", key, "name", "age", "phone")

	beg := time.Now()

	get.AsyncExec(func(ret *kclient.SliceResult) {

		if getAvaDelay == time.Duration(0) {
			getAvaDelay = time.Now().Sub(beg)
		} else {
			getAvaDelay = (time.Now().Sub(beg) + getAvaDelay) / 2
		}

		if ret.ErrCode != nil {

			if ret.ErrCode.Code != errcode.Errcode_record_notexist {
				fmt.Println("get err:", ret.ErrCode)
			}

			if ret.ErrCode.Code == errcode.Errcode_record_notexist {
				fmt.Println("notfound", key)
			}
		}

		atomic.AddInt32(&getCount, 1)
		Get(c)
	})
}

func main() {

	if len(os.Args) < 3 {
		fmt.Println("bin keyrange ip:port ")
		return
	}

	kclient.InitLogger(logger.NewZapLogger("client.log", "./log", "debug", 100, 14, true))

	id = 0

	keyrange, _ = strconv.ParseInt(os.Args[1], 10, 32)

	services := strings.Split(os.Args[2], ",")

	for j := 0; j < 50; j++ {
		c := kclient.OpenClient(services[j%len(services)]).SetUnikeyPlacement(kvnode.MakeUnikeyPlacement([]int{1, 2, 3, 4, 5}))
		for i := 0; i < 50; i++ {
			Get(c)
		}
	}

	go func() {
		for {
			time.Sleep(time.Second)

			getCount_ := atomic.LoadInt32(&getCount)

			fmt.Printf("g:%d,gava:%d\n", getCount_, getAvaDelay/time.Millisecond)

			atomic.StoreInt32(&getCount, 0)

		}
	}()

	//eventQueue.Run()

	sigStop := make(chan bool)
	_, _ = <-sigStop
}
