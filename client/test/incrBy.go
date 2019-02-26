package main

import (
	kclient "flyfish/client"
	"flyfish/errcode"
	"fmt"
	//"github.com/sniperHW/kendynet"
	"github.com/sniperHW/kendynet/golog"
)

func IncrBy(c *kclient.Client) {
	incr := c.IncrBy("counter", "test_counter1", "c", 1)
	incr.Exec(func(ret *kclient.SliceResult) {

		if ret.ErrCode != errcode.ERR_OK {
			fmt.Println(errcode.GetErrorStr(ret.ErrCode))
		} else {
			fmt.Println("c:", ret.Fields["c"].GetInt())
		}
	})
}

func main() {

	kclient.InitLogger(golog.New("flyfish client", golog.NewOutputLogger("log", "flyfish client", 1024*1024*50)))

	services := []string{"127.0.0.1:10012"}
	c := kclient.OpenClient(services) //eventQueue)

	IncrBy(c)

	//eventQueue.Run()

	sigStop := make(chan bool)
	_, _ = <-sigStop
}
