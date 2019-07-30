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

	r1 := c.Del("users1", "sniperHW").Exec()

	if !(r1.ErrCode == errcode.ERR_OK || r1.ErrCode == errcode.ERR_NOTFOUND) {
		fmt.Println("Del error:", errcode.GetErrorStr(r1.ErrCode), r1)
		return
	}

	//不存在技术sniperHW,所以CompareAndSetNx成功
	r2 := c.CompareAndSetNx("users1", "sniperHW", "age", 1, 10).Exec()
	if r2.ErrCode != errcode.ERR_OK {
		fmt.Println("CompareAndSetNx1 error:", errcode.GetErrorStr(r2.ErrCode), r2)
		return
	}

	r3 := c.Get("users1", "sniperHW", "name", "age", "phone").Exec()
	if r3.ErrCode != errcode.ERR_OK {
		fmt.Println("Get Error:", errcode.GetErrorStr(r3.ErrCode), r3)
		return
	}

	fmt.Println(r3.Fields["name"].GetValue(), r3.Fields["age"].GetValue(), r3.Fields["phone"].GetValue())

	//记录sniperHW存在，age != 1所以执行失败
	r4 := c.CompareAndSetNx("users1", "sniperHW", "age", 1, 10).Exec()
	if r4.ErrCode != errcode.ERR_OK {
		fmt.Println("CompareAndSetNx2 error:", errcode.GetErrorStr(r4.ErrCode), r4.Fields["age"].GetValue())
		return
	}
}
