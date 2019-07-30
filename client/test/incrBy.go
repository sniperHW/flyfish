package main

import (
	"fmt"
	kclient "github.com/sniperHW/flyfish/client"
	"github.com/sniperHW/flyfish/errcode"
	"github.com/sniperHW/kendynet/golog"
	"os"
)

func main() {

	kclient.InitLogger(golog.New("flyfish client", golog.NewOutputLogger("log", "flyfish client", 1024*1024*50)))

	services := []string{}

	for i := 1; i < len(os.Args); i++ {
		services = append(services, os.Args[i])
	}

	c := kclient.OpenClient(services)

	fields := map[string]interface{}{}
	fields["age"] = 100
	fields["phone"] = "123456"
	fields["name"] = "sniperHW"

	//不存在技术sniperHW SetNx成功
	r2 := c.Set("users1", "sniperHW", fields).Exec()
	if r2.ErrCode != errcode.ERR_OK {
		fmt.Println("Set error:", errcode.GetErrorStr(r2.ErrCode))
		return
	}

	r3 := c.IncrBy("users1", "sniperHW", "age", 1).Exec()
	if r3.ErrCode != errcode.ERR_OK {
		fmt.Println("IncrBy1 error:", errcode.GetErrorStr(r3.ErrCode))
		return
	}

	fmt.Println(r3.Fields["age"].GetValue())

}
