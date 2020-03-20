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
	fields["age"] = 100
	fields["phone"] = "123456"
	fields["name"] = "sniperHW"

	r2 := c.SetNx("users1", "sniperHW", fields).Exec()
	if !(r2.ErrCode == errcode.ERR_OK || r2.ErrCode == errcode.ERR_RECORD_EXIST) {
		fmt.Println("Set error:", errcode.GetErrorStr(r2.ErrCode))
		return
	}

	r3 := c.DecrBy("users1", "sniperHW", "age", 1).Exec()
	if r3.ErrCode != errcode.ERR_OK {
		fmt.Println("DecrBy1 error:", errcode.GetErrorStr(r3.ErrCode))
		return
	}

	r4 := c.Get("users1", "sniperHW", "age").Exec()
	fmt.Println(r3.Fields["age"].GetInt())
	fmt.Println(r4.Fields["age"].GetInt())

}
