package main

import (
	"fmt"
	"github.com/sniperHW/kendynet/golog"
	"github.com/yddeng/dtest/flyfish"
	"github.com/yddeng/dtest/flyfish/conf"
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

	err := flyfish.Init(_conf)
	if err != nil {
		panic(err)
	}
	fmt.Println("start ok")
}
