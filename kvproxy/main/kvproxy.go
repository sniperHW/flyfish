package main

import (
	"flag"
	"fmt"
	kvproxy "github.com/sniperHW/flyfish/kvproxy"
	futil "github.com/sniperHW/flyfish/util"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	config := flag.String("config", "kvproxy.toml", "config")

	flag.Parse()

	futil.Must(nil, kvproxy.LoadConfig(*config))

	kvproxy.InitLogger()

	proxy := kvproxy.NewKVProxy()
	if nil != proxy && nil == proxy.Start() {
		c := make(chan os.Signal)
		signal.Notify(c, syscall.SIGINT) //监听指定信号
		_ = <-c                          //阻塞直至有信号传入
		fmt.Println("kvproxy stop")
	}
}
