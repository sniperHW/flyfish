package main

import (
	"fmt"
	kclient "github.com/sniperHW/flyfish/client"
	"github.com/sniperHW/flyfish/errcode"
	"github.com/sniperHW/kendynet/golog"
	"os"
	"strings"
)

func Set(c *kclient.Client) {
	fields := map[string]interface{}{}
	fields["age"] = 37
	fields["phone"] = strings.Repeat("a", 64)
	fields["name"] = "sniperHW"

	key := fmt.Sprintf("%s:%d", "huangwei", 1)
	set := c.Set("users1", key, fields)

	set.AsyncExec(func(ret *kclient.StatusResult) {
		fmt.Println(errcode.GetErrorStr(ret.ErrCode), ret.Version)
	})
}

func main() {

	kclient.InitLogger(golog.New("flyfish client", golog.NewOutputLogger("log", "flyfish client", 1024*1024*50)))

	c := kclient.OpenClient(os.Args[1], false)

	Set(c)

	//这三次调用将被合并成一次操作,所以后面这三次返回的version应该是一样的
	Set(c)
	Set(c)
	Set(c)

	sigStop := make(chan bool)
	_, _ = <-sigStop

}
