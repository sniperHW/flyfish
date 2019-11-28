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

	c := kclient.OpenClient(os.Args[1])

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

	r4 := c.MGetAll("users1", "sniperHW1", "sniperHW2", "sniperHW3", "sniperHW4").Exec()

	if r4.ErrCode == errcode.ERR_OK {
		for _, v := range r4.Rows {
			if nil == v.Fields {
				fmt.Println(v.Key, "not exist")
			} else {
				fmt.Println(v.Key, v.Fields["age"].GetInt())
			}
		}
	} else {
		fmt.Println(errcode.GetErrorStr(r4.ErrCode))
	}

}
