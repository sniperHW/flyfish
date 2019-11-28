package main

import (
	"fmt"
	kclient "github.com/sniperHW/flyfish/client"
	"github.com/sniperHW/kendynet/golog"
	"os"
)

func main() {

	if len(os.Args) < 2 {
		fmt.Println("missing ip:port")
		return
	}

	kclient.InitLogger(golog.New("flyfish client", golog.NewOutputLogger("log", "flyfish client", 1024*1024*50)))

	c := kclient.OpenClient(os.Args[1])

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
