package main

import (
	"fmt"
	kclient "github.com/sniperHW/flyfish/client"
	"github.com/sniperHW/flyfish/errcode"
	"github.com/sniperHW/kendynet/golog"
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

func Set(c *kclient.Client) {
	fields := map[string]interface{}{}
	fields["age"] = 37
	fields["phone"] = strings.Repeat("a", 64)
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

	if len(os.Args) < 3 {
		fmt.Println("missing keyrange ip:port")
		return
	}

	//golog.DisableStdOut()
	kclient.InitLogger(golog.New("flyfish client", golog.NewOutputLogger("log", "flyfish client", 1024*1024*50)))

	keyrange, _ = strconv.ParseInt(os.Args[1], 10, 32)

	for j := 0; j < 50; j++ {
		c := kclient.OpenClient(os.Args[2], false)
		for i := 0; i < 50; i++ {
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
