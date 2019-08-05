package main

import (
	"encoding/binary"
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

	buff := make([]byte, 4)

	binary.BigEndian.PutUint32(buff, 100)

	fields := map[string]interface{}{}
	fields["age"] = 100
	fields["blob"] = buff
	fields["name"] = "sniperHW"

	//不存在技术sniperHW SetNx成功
	r2 := c.Set("users1", "sniperHW", fields).Exec()
	if r2.ErrCode != errcode.ERR_OK {
		fmt.Println("Set error:", errcode.GetErrorStr(r2.ErrCode))
		return
	}

	r3 := c.Get("users1", "sniperHW", "blob").Exec()

	fmt.Println(binary.BigEndian.Uint32(r3.Fields["blob"].GetBlob()))

}
