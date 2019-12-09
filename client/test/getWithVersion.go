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

	c := kclient.OpenClient(os.Args[1], false)

	r1 := c.Get("users1", "sniperHW", "name", "phone", "age").Exec()

	r2 := c.GetWithVersion("users1", "sniperHW", r1.Version, "name", "phone", "age").Exec()

	fmt.Println(errcode.GetErrorStr(r2.ErrCode))

	c.Kick("users1", "sniperHW").Exec()

	//kick后将需要从数据库载入

	r2 = c.GetWithVersion("users1", "sniperHW", r1.Version, "name", "phone", "age").Exec()

	fmt.Println(errcode.GetErrorStr(r2.ErrCode))

	//设置
	c.Set("users1", "sniperHW", map[string]interface{}{"age": 13}).Exec()

	//版本号已经发生变更,用老版本号请求会返回数据

	r2 = c.GetWithVersion("users1", "sniperHW", r1.Version, "name", "phone", "age").Exec()

	fmt.Println("req version", r1.Version, "resp version", r2.Version)

	for k, v := range r2.Fields {
		fmt.Println(k, v.GetValue())
	}

}
