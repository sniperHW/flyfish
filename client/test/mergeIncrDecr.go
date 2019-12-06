package main

import (
	"fmt"
	kclient "github.com/sniperHW/flyfish/client"
	"github.com/sniperHW/flyfish/errcode"
	"github.com/sniperHW/kendynet/golog"
	"os"
)

func Incr(c *kclient.Client) {
	c.IncrBy("users1", "sniperHW", "age", 1).AsyncExec(func(ret *kclient.SliceResult) {
		if ret.ErrCode == errcode.ERR_OK {
			fmt.Println(errcode.GetErrorStr(ret.ErrCode), ret.Version, "age:", ret.Fields["age"].GetValue())
		}
	})
}

func Decr(c *kclient.Client) {
	c.DecrBy("users1", "sniperHW", "money", 1).AsyncExec(func(ret *kclient.SliceResult) {

		if ret.ErrCode == errcode.ERR_OK {
			fmt.Println(errcode.GetErrorStr(ret.ErrCode), ret.Version, "money:", ret.Fields["money"].GetValue())
		}
	})
}

func main() {

	kclient.InitLogger(golog.New("flyfish client", golog.NewOutputLogger("log", "flyfish client", 1024*1024*50)))

	c := kclient.OpenClient(os.Args[1], false)

	Incr(c)

	//这三次调用将被合并成一次操作,所以后面这三次返回的version
	Incr(c)
	Decr(c)
	Decr(c)

	sigStop := make(chan bool)
	_, _ = <-sigStop

}
