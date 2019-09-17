package main

import (
	"fmt"
	kclient "github.com/sniperHW/flyfish/client"
	"github.com/sniperHW/flyfish/errcode"
	"github.com/sniperHW/kendynet/golog"
	"os"
	"strconv"
	"sync/atomic"
	"time"
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

		//fmt.Println(ret.Fields["age"].GetInt())

		if getAvaDelay == time.Duration(0) {
			getAvaDelay = time.Now().Sub(beg)
		} else {
			getAvaDelay = (time.Now().Sub(beg) + getAvaDelay) / 2
		}

		if ret.ErrCode != errcode.ERR_OK && ret.ErrCode != errcode.ERR_NOTFOUND {
			fmt.Println("get err:", ret.ErrCode)
		}

		if ret.ErrCode == errcode.ERR_NOTFOUND {
			fmt.Println("notfound", key)
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

	kclient.InitLogger(golog.New("flyfish client", golog.NewOutputLogger("log", "flyfish client", 1024*1024*50)))

	id = 0

	keyrange, _ = strconv.ParseInt(os.Args[1], 10, 32)

	services := []string{}

	for i := 2; i < len(os.Args); i++ {
		services = append(services, os.Args[i])
	}

	for j := 0; j < 100; j++ {
		c := kclient.OpenClient(services) //eventQueue)
		for i := 0; i < 100; i++ {
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
