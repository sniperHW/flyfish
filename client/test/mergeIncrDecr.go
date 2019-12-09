package main

import (
	"fmt"
	kclient "github.com/sniperHW/flyfish/client"
	"github.com/sniperHW/flyfish/errcode"
	"github.com/sniperHW/kendynet/golog"
	"os"
)

func Get(c *kclient.Client) {

	fmt.Println("Get")

	get := c.Get("users1", "sniperHW", "name", "age", "phone", "money")

	get.AsyncExec(func(ret *kclient.SliceResult) {
		if ret.ErrCode == errcode.ERR_OK {
			fmt.Println(ret.Key, ret.Fields["age"].GetInt(), ret.Fields["money"].GetInt())
		} else {
			fmt.Println(errcode.GetErrorStr(ret.ErrCode))
		}
	})
}

func Incr(c *kclient.Client) {
	c.IncrBy("users1", "sniperHW", "age", 1).AsyncExec(func(ret *kclient.StatusResult) {
		if ret.ErrCode == errcode.ERR_OK {
			fmt.Println(errcode.GetErrorStr(ret.ErrCode), ret.Version)
		}
	})
}

func Decr(c *kclient.Client) {
	c.DecrBy("users1", "sniperHW", "money", 1).AsyncExec(func(ret *kclient.StatusResult) {

		if ret.ErrCode == errcode.ERR_OK {
			fmt.Println(errcode.GetErrorStr(ret.ErrCode), ret.Version)
		}
	})
}

func main() {

	kclient.InitLogger(golog.New("flyfish client", golog.NewOutputLogger("log", "flyfish client", 1024*1024*50)))

	c := kclient.OpenClient(os.Args[1], false)

	Get(c)

	Incr(c)

	//这三次调用将被合并成一次操作,所以后面这三次返回的version
	Incr(c)
	Decr(c)
	Decr(c)

	Get(c)

	sigStop := make(chan bool)
	_, _ = <-sigStop

}
