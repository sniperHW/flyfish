package main

import (
	"fmt"
	kclient "github.com/sniperHW/flyfish/client"
	"github.com/sniperHW/flyfish/errcode"
	"github.com/sniperHW/kendynet/golog"
	"math/rand"
	"os"
	"strings"
	"sync/atomic"
	"time"
)

var (
	setCount     int32
	timeoutCount int32
	busyCount    int32
	setAvaDelay  time.Duration
	id           int64
)

func Set(c *kclient.Client) {
	fields := map[string]interface{}{}
	fields["age"] = 37
	fields["phone"] = strings.Repeat("a", 1024)
	fields["name"] = "sniperHW"
	key := fmt.Sprintf("%s:%d", "huangwei", rand.Int()%100000)
	//key := fmt.Sprintf("%s:%d","huangwei",id%100000)//rand.Int()%1000000)
	//key := "huangwei:44745"
	id++
	set := c.Set("users1", key, fields)

	beg := time.Now()

	set.AsyncExec(func(ret *kclient.StatusResult) {

		if setAvaDelay == time.Duration(0) {
			setAvaDelay = time.Now().Sub(beg)
		} else {
			setAvaDelay = (time.Now().Sub(beg) + setAvaDelay) / 2
		}

		if ret.ErrCode != errcode.ERR_OK {
			if ret.ErrCode == errcode.ERR_TIMEOUT {
				atomic.AddInt32(&timeoutCount, 1)
			} else if ret.ErrCode == errcode.ERR_BUSY {
				atomic.AddInt32(&busyCount, 1)
			} else {
				fmt.Println("set err:", ret.ErrCode, key)
			}

			//fmt.Println("set err:", ret.ErrCode, key)
			//kclient.Debugln("set err:", ret.ErrCode, key)
		}
		atomic.AddInt32(&setCount, 1)
		Set(c)
	})
}

func main() {

	if len(os.Args) < 2 {
		fmt.Println("missing ip:port")
		return
	}

	//golog.DisableStdOut()
	kclient.InitLogger(golog.New("flyfish client", golog.NewOutputLogger("log", "flyfish client", 1024*1024*50)))

	id = 0

	services := []string{}

	for i := 1; i < len(os.Args); i++ {
		services = append(services, os.Args[i])
	}

	for j := 0; j < 10; j++ {
		c := kclient.OpenClient(services)
		for i := 0; i < 10; i++ {
			Set(c)
		}
	}

	go func() {
		for {
			time.Sleep(time.Second)
			setCount_ := atomic.LoadInt32(&setCount)
			timeoutCount_ := atomic.LoadInt32(&timeoutCount)
			busyCount_ := atomic.LoadInt32(&busyCount)

			fmt.Printf("s:%d,sava:%d,timeout:%d,busy:%d\n", setCount_, setAvaDelay/time.Millisecond, timeoutCount_, busyCount_)
			atomic.StoreInt32(&setCount, 0)
			atomic.StoreInt32(&timeoutCount, 0)
			atomic.StoreInt32(&busyCount, 0)
		}
	}()

	sigStop := make(chan bool)
	_, _ = <-sigStop
}
