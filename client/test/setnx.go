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

	r1 := c.Del("users1", "sniperHW").Exec()

	if !(r1.ErrCode == errcode.ERR_OK || r1.ErrCode == errcode.ERR_RECORD_NOTEXIST) {
		fmt.Println("Del error:", errcode.GetErrorStr(r1.ErrCode), r1)
		return
	}

	fields := map[string]interface{}{}
	fields["age"] = 1
	fields["phone"] = "123456"
	fields["name"] = "sniperHW"

	//不存在技术sniperHW SetNx成功
	r2 := c.SetNx("users1", "sniperHW", fields).Exec()
	if r2.ErrCode != errcode.ERR_OK {
		fmt.Println("SetNx1 error:", errcode.GetErrorStr(r2.ErrCode), r2)
		return
	}

	r3 := c.Get("users1", "sniperHW", "name", "age", "phone").Exec()
	if r3.ErrCode != errcode.ERR_OK {
		fmt.Println("Get Error:", errcode.GetErrorStr(r3.ErrCode), r3)
		return
	}

	fmt.Println(r3.Fields["name"].GetValue(), r3.Fields["age"].GetValue(), r3.Fields["phone"].GetValue())

	//记录sniperHW存在执行失败
	r4 := c.SetNx("users1", "sniperHW", fields).Exec()
	if r4.ErrCode != errcode.ERR_OK {
		fmt.Println("SetNx2 error:", errcode.GetErrorStr(r4.ErrCode))
		return
	}
}
