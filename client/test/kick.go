package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/schollz/progressbar"
	kclient "github.com/sniperHW/flyfish/client"
	"github.com/sniperHW/flyfish/logger"
	"github.com/sniperHW/flyfish/server/kvnode"
)

var (
	id       int64
	total    int64
	sigStop  chan bool = make(chan bool)
	keyrange int64
	bar      *progressbar.ProgressBar
)

func Kick(c *kclient.Client) bool {

	nextID := atomic.AddInt64(&id, 1)
	if nextID > total {
		return false
	}

	key := fmt.Sprintf("%s:%d", "huangwei", nextID%keyrange)
	kick := c.Kick("users1", key)

	kick.AsyncExec(func(ret *kclient.StatusResult) {
		if nextID == total {
			sigStop <- true
		}
		bar.Add(1)
	})
	return true
}

func main() {

	if len(os.Args) < 3 {
		fmt.Println("bin keyrange count ip:port")
		return
	}

	keyrange, _ = strconv.ParseInt(os.Args[1], 10, 32)
	total, _ = strconv.ParseInt(os.Args[2], 10, 32)

	kclient.InitLogger(logger.NewZapLogger("client.log", "./log", "debug", 100, 14, 10, true))

	id = 0

	service := os.Args[3]

	bar = progressbar.New(int(total))

	for j := 0; j < 100; j++ {
		c, _ := kclient.OpenClient(kclient.ClientConf{SoloService: service, UnikeyPlacement: flykv.MakeUnikeyPlacement([]int{1, 2, 3, 4, 5})})
		go func() {
			for {
				for i := 0; i < 50; i++ {
					if !Kick(c) {
						return
					}
				}
				time.Sleep(time.Millisecond * 50)
			}
		}()
	}

	sigStop = make(chan bool)
	_, _ = <-sigStop
}
