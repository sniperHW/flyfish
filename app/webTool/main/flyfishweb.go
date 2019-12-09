package main

import (
	"fmt"
	"github.com/sniperHW/flyfish/app/webTool"
	"github.com/sniperHW/flyfish/app/webTool/conf"
	"github.com/sniperHW/kendynet/golog"
	"os"
)

var logger *golog.Logger

func main() {
	if len(os.Args) < 1 {
		fmt.Printf("usage config\n")
		return
	}

	conf.LoadConfig(os.Args[1])
	_conf := conf.GetConfig()

	err := webTool.Init(_conf)
	if err != nil {
		panic(err)
	}
	fmt.Println("start ok")
}
