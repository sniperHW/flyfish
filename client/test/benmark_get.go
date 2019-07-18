package main

import (
	kclient "flyfish/client"
	"flyfish/errcode"
	"fmt"
	"github.com/sniperHW/kendynet/golog"
	"math/rand"
	"os"
	"sync/atomic"
	"time"
)

var (
	getCount    int32
	getAvaDelay time.Duration
	id          int64
)

func Get(c *kclient.Client) {

	key := fmt.Sprintf("%s:%d", "huangwei", rand.Int()%100000)
	//key := fmt.Sprintf("%s:%d","huangwei",id%1000000)//rand.Int()%1000000)
	id++
	get := c.Get("users1", key, "name", "age", "phone")

	beg := time.Now()

	get.Exec(func(ret *kclient.SliceResult) {

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

	if len(os.Args) < 2 {
		fmt.Println("missing ip:port")
		return
	}

	kclient.InitLogger(golog.New("flyfish client", golog.NewOutputLogger("log", "flyfish client", 1024*1024*50)))

	id = 0

	services := []string{}

	for i := 1; i < len(os.Args); i++ {
		services = append(services, os.Args[i])
	}

	for j := 0; j < 10; j++ {
		c := kclient.OpenClient(services) //eventQueue)
		for i := 0; i < 20; i++ {
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
