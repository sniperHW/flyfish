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

	fields := map[string]interface{}{}

	fields["age"] = 1
	fields["phone"] = "123456"
	fields["name"] = "sniperHW1"

	r1 := c.Set("users1", "sniperHW1", fields).Exec()
	if r1.ErrCode != errcode.ERR_OK {
		fmt.Println("Set1 error:", errcode.GetErrorStr(r1.ErrCode), r1)
		return
	}

	fields["age"] = 2
	fields["phone"] = "123456"
	fields["name"] = "sniperHW2"

	r2 := c.Set("users1", "sniperHW2", fields).Exec()
	if r2.ErrCode != errcode.ERR_OK {
		fmt.Println("Set2 error:", errcode.GetErrorStr(r2.ErrCode), r2)
		return
	}

	fields["age"] = 3
	fields["phone"] = "123456"
	fields["name"] = "sniperHW3"

	r3 := c.Set("users1", "sniperHW3", fields).Exec()
	if r3.ErrCode != errcode.ERR_OK {
		fmt.Println("Set3 error:", errcode.GetErrorStr(r3.ErrCode), r3)
		return
	}

	r4 := kclient.MGet(nil, c.GetAll("users1", "sniperHW1"), c.GetAll("users1", "sniperHW2"), c.GetAll("users1", "sniperHW3"), c.GetAll("users1", "sniperHW4")).Exec()

	for _, v := range r4 {
		if v.ErrCode == errcode.ERR_OK {
			fmt.Println(v.Table, v.Key, "age:", v.Fields["age"].GetInt())
		} else {
			fmt.Println(v.Table, v.Key, errcode.GetErrorStr(v.ErrCode))
		}
	}

}
