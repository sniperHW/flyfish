package main

import (
	"fmt"
	kclient "github.com/sniperHW/flyfish/client"
	"github.com/sniperHW/flyfish/client/test/config"
	"github.com/sniperHW/flyfish/errcode"
	"github.com/sniperHW/flyfish/logger"
	"github.com/sniperHW/flyfish/server/slot"
	"os"
	"strconv"
	"strings"
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

	if len(os.Args) < 2 {
		fmt.Println("bin keyrange")
		return
	}

	cfg, err := config.LoadConfig("./config.toml")

	if nil != err {
		panic(err)
	}

	kclient.InitLogger(logger.NewZapLogger("client.log", "./log", "debug", 100, 14, 10, true))

	var clientCfg kclient.ClientConf

	if cfg.Mode == "solo" {
		clientCfg.SoloService = cfg.Service
		clientCfg.UnikeyPlacement = slot.MakeUnikeyPlacement(cfg.Stores)
	} else {
		clientCfg.PD = strings.Split(cfg.PD, ";")
	}

	keyrange, _ = strconv.ParseInt(os.Args[1], 10, 32)

	for j := 0; j < 50; j++ {
		c, _ := kclient.OpenClient(clientCfg)
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
