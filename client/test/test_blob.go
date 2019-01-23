package main

import (
	kclient "flyfish/client"
	"flyfish/errcode"
	"fmt"
	"github.com/sniperHW/kendynet/golog"
)

func Get(c *kclient.Client) {
	get := c.Get("blob", "blob4", "data")

	get.Exec(func(ret *kclient.SliceResult) {
		if ret.ErrCode == errcode.ERR_OK {
			fmt.Println("data", string(ret.Fields["data"].GetBlob()))
		} else {
			fmt.Println(errcode.GetErrorStr(ret.ErrCode))
		}
	})
}

func Set(c *kclient.Client) {
	fields := map[string]interface{}{}
	//fields["data"] = ([]byte)("blob3")

	fields["name"] = "blob4"

	set := c.Set("blob", "blob4", fields)
	set.Exec(func(ret *kclient.StatusResult) {
		if ret.ErrCode != errcode.ERR_OK {
			fmt.Println(errcode.GetErrorStr(ret.ErrCode))
		} else {
			fmt.Println("set ok")
			Get(c)
		}
	})
}

func CompareAndSet(c *kclient.Client) {
	set := c.CompareAndSet("blob", "blob1", "data", ([]byte)("haha"), ([]byte)("blob1"))
	set.Exec(func(ret *kclient.SliceResult) {
		if ret.ErrCode != errcode.ERR_OK {
			fmt.Println(errcode.GetErrorStr(ret.ErrCode), ret)
		} else {
			fmt.Println("data", ret.Fields["data"].GetBlob())
			fmt.Println("set ok")
		}
	})
}

func main() {

	kclient.InitLogger(golog.NewOutputLogger("log", "flyfish client", 1024*1024*50), "error")

	services := []string{"127.0.0.1:10012"}

	c := kclient.OpenClient(services) //eventQueue)
	Set(c)

	//CompareAndSet(c)
	//Set(c,2)
	//Set(c,3)
	//Set(c,4)

	//eventQueue.Run()

	sigStop := make(chan bool)
	_, _ = <-sigStop
}
