package main

import (
	kclient "flyfish/client"
	"flyfish/errcode"
	"fmt"
	"github.com/sniperHW/kendynet/golog"
)

func DecrBy(c *kclient.Client) {
	decr := c.DecrBy("counter", "test_counter2", "c", 1)
	decr.Exec(func(ret *kclient.SliceResult) {

		if ret.ErrCode != errcode.ERR_OK {
			fmt.Println(errcode.GetErrorStr(ret.ErrCode))
		} else {
			fmt.Println("c:", ret.Fields["c"].GetInt())
		}
	})
}

func main() {

	kclient.InitLogger(golog.NewOutputLogger("log", "flyfish client", 1024*1024*50), "error")

	services := []string{"127.0.0.1:10012"}
	c := kclient.OpenClient(services) //eventQueue)

	DecrBy(c)

	//eventQueue.Run()

	sigStop := make(chan bool)
	_, _ = <-sigStop
}
