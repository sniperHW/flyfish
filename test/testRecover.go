package main

import (
	"flyfish"
	//"flyfish/conf"
	//"fmt"
	//"github.com/go-ini/ini"
	//"github.com/sniperHW/kendynet/golog"
	//"net/http"
	//_ "net/http/pprof"
	//"os"
	//"os/signal"
	//"syscall"
)

func main() {
	flyfish.InitLogger()
	flyfish.TestRecover()
}
