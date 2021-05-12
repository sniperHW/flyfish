package main

import (
	"fmt"
	kclient "github.com/sniperHW/flyfish/client"
	"github.com/sniperHW/flyfish/errcode"
	"github.com/sniperHW/flyfish/logger"
	"math/rand"
	"os"
	"strconv"
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

var keyrange int64

var id int64

func Set(c *kclient.Client) {
	fields := map[string]interface{}{}
	fields["age"] = 37
	fields["phone"] = strings.Repeat("a", 1024)
	fields["name"] = "sniperHW"
	key := fmt.Sprintf("%s:%d", "huangwei", rand.Int()%int(keyrange))
	id++
	set := c.Set("users1", key, fields)

	beg := time.Now()

	set.AsyncExec(func(ret *kclient.StatusResult) {

		if setAvaDelay == time.Duration(0) {
			setAvaDelay = time.Now().Sub(beg)
		} else {
			setAvaDelay = (time.Now().Sub(beg) + setAvaDelay) / 2
		}

		if ret.ErrCode != nil {
			fmt.Println("set err:", errcode.GetErrorDesc(ret.ErrCode), key)
		}
		atomic.AddInt32(&setCount, 1)
		Set(c)
	})
}

func Get(c *kclient.Client) {

	key := fmt.Sprintf("%s:%d", "huangwei", rand.Int()%int(keyrange))

	id++
	get := c.Get("users1", key, "name", "age", "phone")

	beg := time.Now()

	get.AsyncExec(func(ret *kclient.SliceResult) {

		if getAvaDelay == time.Duration(0) {
			getAvaDelay = time.Now().Sub(beg)
		} else {
			getAvaDelay = (time.Now().Sub(beg) + getAvaDelay) / 2
		}

		if ret.ErrCode != nil && ret.ErrCode.Code != errcode.Errcode_record_notexist {
			fmt.Println("get err:", ret.ErrCode)
		}

		if ret.ErrCode.Code == errcode.Errcode_record_notexist {
			fmt.Println("notfound", key)
		}

		atomic.AddInt32(&getCount, 1)
		Get(c)
	})
}

func main() {

	if len(os.Args) < 3 {
		fmt.Println("bin keyrange ip:port")
		return
	}

	keyrange, _ = strconv.ParseInt(os.Args[1], 10, 32)

	kclient.InitLogger(logger.NewZapLogger("client.log", "./log", "debug", 100, 14, true))

	id = 0

	for j := 0; j < 100; j++ {
		c := kclient.OpenClient(os.Args[2], false) //eventQueue)
		for i := 0; i < 10; i++ {
			Set(c)
		}
	}

	for j := 0; j < 50; j++ {
		c := kclient.OpenClient(os.Args[2], false) //eventQueue)
		for i := 0; i < 20; i++ {
			Get(c)
		}
	}

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
