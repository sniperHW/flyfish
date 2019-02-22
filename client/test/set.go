package main

import (
	kclient "flyfish/client"
	"flyfish/errcode"
	"fmt"
	//"github.com/sniperHW/kendynet"
	"github.com/sniperHW/kendynet/golog"
	"strings"
)

func Set(c *kclient.Client, i int) {
	fields := map[string]interface{}{}
	fields["age"] = 37
	fields["phone"] = strings.Repeat("a", 1024)
	fields["name"] = "sniperHW"
	key := fmt.Sprintf("%s:%d", "huangwei", i)

	set := c.Set("users1", key, fields)
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

	Set(c, 1)
	//Set(c,2)
	//Set(c,3)
	//Set(c,4)

	//eventQueue.Run()

	sigStop := make(chan bool)
	_, _ = <-sigStop
}
