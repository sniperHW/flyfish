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
	fields["age"] = 1
	fields["phone"] = "123456"
	fields["name"] = "sniperHW"

	r1 := c.Set("users1", "sniperHW", fields).Exec()

	if r1.ErrCode != errcode.ERR_OK {
		fmt.Println(errcode.GetErrorStr(r1.ErrCode), r1)
		return
	}

	r2 := c.CompareAndSet("users1", "sniperHW", "age", 1, 10).Exec()
	if r2.ErrCode != errcode.ERR_OK {
		fmt.Println(errcode.GetErrorStr(r2.ErrCode), r2)
		return
	}

	r3 := c.Get("users1", "sniperHW", "name", "age", "phone").Exec()
	if r3.ErrCode != errcode.ERR_OK {
		fmt.Println(errcode.GetErrorStr(r3.ErrCode), r3)
		return
	}

	fmt.Println(r3.Fields["name"].GetValue(), r3.Fields["age"].GetValue(), r3.Fields["phone"].GetValue())

	r4 := c.CompareAndSet("users1", "sniperHW", "age", 1, 10).Exec()
	if r4.ErrCode != errcode.ERR_OK {
		fmt.Println(errcode.GetErrorStr(r4.ErrCode), r4.Fields["age"].GetValue())
		return
	}
}
