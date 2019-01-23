package main

import (
	kclient "flyfish/client"
	"flyfish/errcode"
	"fmt"
	"github.com/sniperHW/kendynet/golog"
	"math/rand"
	"strings"
	"sync/atomic"
	"time"
)

var (
	setCount    int32
	setAvaDelay time.Duration
	id          int64
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

	set.Exec(func(ret *kclient.StatusResult) {

		if setAvaDelay == time.Duration(0) {
			setAvaDelay = time.Now().Sub(beg)
		} else {
			setAvaDelay = (time.Now().Sub(beg) + setAvaDelay) / 2
		}

		if ret.ErrCode != errcode.ERR_OK {
			fmt.Println("set err:", ret.ErrCode, key)
			kclient.Debugln("set err:", ret.ErrCode, key)
		}
		atomic.AddInt32(&setCount, 1)
		Set(c)
	})
}

func main() {

	//golog.DisableStdOut()
	kclient.InitLogger(golog.NewOutputLogger("log", "flyfish client", 1024*1024*50), "error")

	id = 0

	services := []string{"127.0.0.1:10012"} //,"127.0.0.1:10013"}

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
			fmt.Printf("s:%d,sava:%d\n", setCount_, setAvaDelay/time.Millisecond)
			atomic.StoreInt32(&setCount, 0)
		}
	}()

	sigStop := make(chan bool)
	_, _ = <-sigStop
}
