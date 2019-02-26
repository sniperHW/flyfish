package main

import (
	kclient "flyfish/client"
	"flyfish/errcode"
	"fmt"
	"github.com/sniperHW/kendynet/golog"
)

func CompareAndSet(c *kclient.Client) {
	set := c.CompareAndSet("counter", "test_counter1", "c", 2, 100)
	set.Exec(func(ret *kclient.SliceResult) {

		if ret.ErrCode != errcode.ERR_OK {
			fmt.Println(errcode.GetErrorStr(ret.ErrCode), ret)
		} else {
			fmt.Println("set ok")
		}
	})
}

func main() {

	kclient.InitLogger(golog.New("flyfish client", golog.NewOutputLogger("log", "flyfish client", 1024*1024*50)))

	services := []string{"127.0.0.1:10012"}
	c := kclient.OpenClient(services) //eventQueue)

	CompareAndSet(c)

	//eventQueue.Run()

	sigStop := make(chan bool)
	_, _ = <-sigStop
}
