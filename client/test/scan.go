package main

import (
	kclient "flyfish/client"
	"fmt"
	"github.com/sniperHW/kendynet/golog"
	"os"
)

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

	c := kclient.OpenClient(services)

	scaner := c.Scaner("users1", "age")

	for i := 0; i < 10; i++ {
		r, err := scaner.Next(10)
		if nil == err {
			for _, v := range r.Rows {
				fmt.Println(v.Key)
			}
		} else {
			fmt.Println(err)
			break
		}
	}
	scaner.Close()
}
