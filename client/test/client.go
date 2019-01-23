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

var getCount int32
var setCount int32
var delCount int32

var getAvaDelay time.Duration
var setAvaDelay time.Duration
var delAvaDelay time.Duration

var id int64

func Set(c *kclient.Client) {
	fields := map[string]interface{}{}
	fields["age"] = 37
	fields["phone"] = strings.Repeat("a", 1024)
	fields["name"] = "sniperHW"
	key := fmt.Sprintf("%s:%d", "huangwei", rand.Int()%50000)
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
		//Set(c)
	})
}

func Get(c *kclient.Client) {

	key := fmt.Sprintf("%s:%d", "huangwei", rand.Int()%50000+50000)
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

	//golog.DisableStdOut()
	kclient.InitLogger(golog.NewOutputLogger("log", "flyfish client", 1024*1024*50), "error")

	id = 0

	services := []string{"127.0.0.1:10012"} //,"127.0.0.1:10013"}

	for j := 0; j < 10; j++ {
		c := kclient.OpenClient(services) //eventQueue)
		for i := 0; i < 10; i++ {
			Set(c)
		}
	}

	/*for j := 0; j < 10; j++ {
		c := kclient.OpenClient(services) //eventQueue)
		for i := 0; i < 20; i++ {
			Get(c)
		}
	}*/

	go func() {
		for {
			time.Sleep(time.Second)
			setCount_ := atomic.LoadInt32(&setCount)
			getCount_ := atomic.LoadInt32(&getCount)
			delCount_ := atomic.LoadInt32(&delCount)
			fmt.Printf("s:%d,sava:%d,g:%d,gava:%d,d:%d,dava:%d,total:%d\n",
				setCount_,
				setAvaDelay/time.Millisecond,
				getCount_,
				getAvaDelay/time.Millisecond,
				delCount_,
				delAvaDelay/time.Millisecond,
				(setCount_ + getCount_ + delCount_))

			atomic.StoreInt32(&setCount, 0)
			atomic.StoreInt32(&getCount, 0)
			atomic.StoreInt32(&delCount, 0)
		}
	}()

	//eventQueue.Run()

	sigStop := make(chan bool)
	_, _ = <-sigStop
}
