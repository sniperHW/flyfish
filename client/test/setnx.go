package main

import (
	kclient "flyfish/client"
	"flyfish/errcode"
	"fmt"
	"github.com/sniperHW/kendynet"
	"github.com/sniperHW/kendynet/golog"
	"strings"
)

func SetNx(c *kclient.Client) {
	fields := map[string]interface{}{}
	fields["age"] = 37
	fields["phone"] = strings.Repeat("a", 1024)
	fields["name"] = "sniperHW"
	key := fmt.Sprintf("%s:%d", "xiba", 10)

	set := c.SetNx("users1", key, fields)
	set.Exec(func(ret *kclient.StatusResult) {

		if ret.ErrCode != errcode.ERR_OK {
			fmt.Println(errcode.GetErrorStr(ret.ErrCode))
		} else {
			fmt.Println("set ok")
		}
	})
}

func main() {

	kclient.InitLogger(golog.New("flyfish client", golog.NewOutputLogger("log", "flyfish client", 1024*1024*50)), "debug")

	services := []string{"127.0.0.1:10012"}
	c := kclient.OpenClient(services) //eventQueue)

	SetNx(c)

	//eventQueue.Run()

	sigStop := make(chan bool)
	_, _ = <-sigStop
}
