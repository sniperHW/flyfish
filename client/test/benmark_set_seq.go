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
	setCount     int32
	timeoutCount int32
	busyCount    int32
	setAvaDelay  time.Duration
	id           int64
	keyrange     int64
)

var phone []byte = []byte(strings.Repeat("a", 16*1024))

func Set(c *kclient.Client) {
	fields := map[string]interface{}{}
	fields["age"] = 37
	fields["phone"] = phone
	fields["name"] = "sniperHW"
	nextID := atomic.AddInt64(&id, 1)
	key := fmt.Sprintf("%s:%d", "huangwei", nextID%keyrange)
	set := c.Set("users1", key, fields)

	beg := time.Now()

	set.AsyncExec(func(ret *kclient.StatusResult) {

		if setAvaDelay == time.Duration(0) {
			setAvaDelay = time.Now().Sub(beg)
		} else {
			setAvaDelay = (time.Now().Sub(beg) + setAvaDelay) / 2
		}

		if ret.ErrCode != nil {
			if ret.ErrCode.Code == errcode.Errcode_timeout {
				atomic.AddInt32(&timeoutCount, 1)
			} else if ret.ErrCode.Code == errcode.Errcode_retry {
				atomic.AddInt32(&busyCount, 1)
			} else {
				fmt.Println("set err:", errcode.GetErrorDesc(ret.ErrCode), key)
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
		fmt.Println("missing keyrange")
		return
	}

	cfg, err := config.LoadConfig("./config.toml")

	if nil != err {
		panic(err)
	}

	kclient.InitLogger(logger.NewZapLogger("client.log", "./log", "debug", 100, 14, 10, true))

	keyrange, _ = strconv.ParseInt(os.Args[1], 10, 32)

	var clientCfg kclient.ClientConf

	if cfg.ClientType == "FlyKv" {
		clientCfg.ClientType = kclient.ClientType_FlyKv
	} else {
		clientCfg.ClientType = kclient.ClientType_FlySql
	}

	if cfg.Mode == "solo" {
		clientCfg.SoloConf = &kclient.SoloConf{
			Service:         cfg.Service,
			UnikeyPlacement: slot.MakeUnikeyPlacement(cfg.Stores),
		}
	} else {
		clientCfg.ClusterConf = &kclient.ClusterConf{
			PD: strings.Split(cfg.PD, ";"),
		}
	}

	for j := 0; j < 1; j++ {
		c, _ := kclient.OpenClient(clientCfg)
		for i := 0; i < 200; i++ {
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
