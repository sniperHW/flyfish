package main

import (
	kclient "flyfish/client"
	"flyfish/errcode"
	"fmt"

	//"github.com/sniperHW/kendynet"
	//"strings"

	"github.com/sniperHW/kendynet/golog"
)

func Set(c *kclient.Client, i int) {
	fields := map[string]interface{}{}
	fields["battle_balance"] = "haha"
	key := fmt.Sprintf("%s:%d", "huangwei", i)

	set := c.Set("role_battle_balance", key, fields)
	set.Exec(func(ret *kclient.StatusResult) {

		if ret.ErrCode != errcode.ERR_OK {
			fmt.Println(errcode.GetErrorStr(ret.ErrCode))
		} else {
			fmt.Println("set ok")
		}
	})
}

func SetSync(c *kclient.Client, i int) {
	fields := map[string]interface{}{}
	fields["battle_balance"] = "haha"
	key := fmt.Sprintf("%s:%d", "huangwei", i)

	set := c.SetSync("role_battle_balance", key, fields)
	set.Exec(func(ret *kclient.StatusResult) {

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

	Set(c, 1)
	SetSync(c, 1)
	//Set(c, 1)
	//Set(c, 1)
	//Set(c, 1)
	//Set(c, 1)

	//Del(c, 1)
	//Del(c, 2)

	//Set(c,3)
	//Set(c,4)

	//eventQueue.Run()

	sigStop := make(chan bool)
	_, _ = <-sigStop
}
