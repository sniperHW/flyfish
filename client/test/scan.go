package main

import (
	kclient "flyfish/client"
	"flyfish/errcode"
	"fmt"
	"github.com/sniperHW/kendynet/golog"
	"os"
)

var c int32 = 0

func scanCb(scaner *kclient.Scaner, ret *kclient.MutiResult) {

	if ret.ErrCode == errcode.ERR_OK {

		for _, v := range ret.Rows {
			fmt.Println(v.Key)
			c++
		}

		if c > 100 {
			scaner.Close()
			fmt.Println("scan 100 rows")
			return
		}

		scaner.Next(10, scanCb)

	} else {

		fmt.Println(errcode.GetErrorStr(ret.ErrCode))

		scaner.Close()
	}
}

func main() {

	if len(os.Args) < 2 {
		fmt.Println("missing ip:port")
		return
	}

	kclient.InitLogger(golog.New("flyfish client", golog.NewOutputLogger("log", "flyfish client", 1024*1024*50)))

	services := []string{}

	for i := 1; i < len(os.Args); i++ {
		services = append(services, os.Args[i])
	}

	c := kclient.OpenClient(services) //eventQueue)

	scaner := c.Scaner("users1", "age")

	scaner.Next(10, scanCb)

	sigStop := make(chan bool)
	_, _ = <-sigStop
}
