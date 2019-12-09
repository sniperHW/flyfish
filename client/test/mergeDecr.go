package main

import (
	"fmt"
	kclient "github.com/sniperHW/flyfish/client"
	"github.com/sniperHW/flyfish/errcode"
	"github.com/sniperHW/kendynet/golog"
	"os"
)

func Decr(c *kclient.Client) {
	c.DecrBy("users1", "sniperHW", "age", 1).AsyncExec(func(ret *kclient.StatusResult) {

		if ret.ErrCode == errcode.ERR_OK {
			fmt.Println(errcode.GetErrorStr(ret.ErrCode), ret.Version)
		}
	})
}

func main() {

	kclient.InitLogger(golog.New("flyfish client", golog.NewOutputLogger("log", "flyfish client", 1024*1024*50)))

	c := kclient.OpenClient(os.Args[1], false)

	Decr(c)

	//这三次调用将被合并成一次操作即-3,所以后面这三次返回的version应该是一样的
	Decr(c)
	Decr(c)
	Decr(c)

	sigStop := make(chan bool)
	_, _ = <-sigStop

}
