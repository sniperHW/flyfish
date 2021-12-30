package main

import (
	"fmt"
	kclient "github.com/sniperHW/flyfish/client"
	"github.com/sniperHW/flyfish/client/test/config"
	"github.com/sniperHW/flyfish/errcode"
	"github.com/sniperHW/flyfish/logger"
	"github.com/sniperHW/flyfish/server/flykv"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

var getCount int32
var setCount int32

var getAverDelay time.Duration
var setAverDelay time.Duration

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

		if setAverDelay == time.Duration(0) {
			setAverDelay = time.Now().Sub(beg)
		} else {
			setAverDelay = (time.Now().Sub(beg) + setAverDelay) / 2
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

		if getAverDelay == time.Duration(0) {
			getAverDelay = time.Now().Sub(beg)
		} else {
			getAverDelay = (time.Now().Sub(beg) + getAverDelay) / 2
		}

		if ret.ErrCode != nil && ret.ErrCode.Code != errcode.Errcode_record_notexist {
			fmt.Println("get err:", ret.ErrCode)
		}

		atomic.AddInt32(&getCount, 1)
		Get(c)
	})
}

func main() {

	if len(os.Args) < 2 {
		fmt.Println("bin keyrange")
		return
	}

	cfg, err := config.LoadConfig("./config.toml")

	if nil != err {
		panic(err)
	}

	keyrange, _ = strconv.ParseInt(os.Args[1], 10, 32)

	kclient.InitLogger(logger.NewZapLogger("client.log", "./log", "debug", 100, 14, 10, true))

	var clientCfg kclient.ClientConf

	if cfg.Mode == "solo" {
		clientCfg.SoloService = cfg.Service
		clientCfg.UnikeyPlacement = flykv.MakeUnikeyPlacement(cfg.Stores)
	} else {
		clientCfg.PD = strings.Split(cfg.PD, ";")
	}

	for j := 0; j < 100; j++ {
		c, _ := kclient.OpenClient(clientCfg)
		for i := 0; i < 10; i++ {
			Set(c)
		}
	}

	for j := 0; j < 50; j++ {
		c, _ := kclient.OpenClient(clientCfg)
		for i := 0; i < 20; i++ {
			Get(c)
		}
	}

	go func() {
		for {
			time.Sleep(time.Second)
			setCount_ := atomic.LoadInt32(&setCount)
			getCount_ := atomic.LoadInt32(&getCount)
			fmt.Printf("set:%d,setAver:%d,get:%d,getAver:%d,total:%d\n",
				setCount_,
				setAverDelay/time.Millisecond,
				getCount_,
				getAverDelay/time.Millisecond,
				(setCount_ + getCount_))

			atomic.StoreInt32(&setCount, 0)
			atomic.StoreInt32(&getCount, 0)
		}
	}()

	//eventQueue.Run()

	sigStop := make(chan bool)
	_, _ = <-sigStop
}
