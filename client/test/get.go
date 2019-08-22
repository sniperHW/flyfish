package main

import (
	"fmt"
	kclient "github.com/sniperHW/flyfish/client"
	"github.com/sniperHW/flyfish/errcode"

	//"github.com/sniperHW/kendynet"
	"github.com/sniperHW/kendynet/golog"
)

func Get(c *kclient.Client, i int) {

	key := fmt.Sprintf("%s:%d", "huangwei", i) //rand.Int()%100000)
	get := c.Get("users1", key, "name", "age", "phone")

	get.AsyncExec(func(ret *kclient.SliceResult) {
		if ret.ErrCode == errcode.ERR_OK {
			fmt.Println(ret.Key, ret.Fields["age"].GetInt())
		} else {
			fmt.Println(errcode.GetErrorStr(ret.ErrCode))
		}
	})
}

func Set(c *kclient.Client, i int) {

	fields := map[string]interface{}{}
	fields["name"] = "haha"
	fields["age"] = i
	fields["phone"] = "12345"

	key := fmt.Sprintf("%s:%d", "huangwei", i)

	set := c.Set("users1", key, fields)
	set.AsyncExec(func(ret *kclient.StatusResult) {

		if ret.ErrCode != errcode.ERR_OK {
			fmt.Println(errcode.GetErrorStr(ret.ErrCode))
		} else {
			fmt.Println("set ok")
		}
	})
}

func main() {

	kclient.InitLogger(golog.New("flyfish client", golog.NewOutputLogger("log", "flyfish client", 1024*1024*50)))

	services := []string{"127.0.0.1:10012"}
	c := kclient.OpenClient(services) //eventQueue)

	/*Set(c, 1)
	Set(c, 2)
	Set(c, 3)
	Set(c, 4)*/

	Get(c, 1)
	Get(c, 2)
	Get(c, 3)
	Get(c, 4)

	//eventQueue.Run()

	sigStop := make(chan bool)
	_, _ = <-sigStop
}
