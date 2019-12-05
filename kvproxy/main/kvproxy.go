package main

import (
	"flag"
	"fmt"
	kvproxy "github.com/sniperHW/flyfish/kvproxy"
	"os"
	"os/signal"
	"strings"
	"syscall"
)

func main() {
	addr := flag.String("addr", "0.0.0.0:8080", "addr of proxy")
	kvnodes := flag.String("kvnodes", "", "kvnodes id1:ip1:port1,id2:ip2:port2...")

	flag.Parse()

	fmt.Println(*addr, *kvnodes, strings.Split(*kvnodes, ","))

	proxy := kvproxy.NewKVProxy(*addr, 4, strings.Split(*kvnodes, ","))
	if nil != proxy && nil == proxy.Start() {
		c := make(chan os.Signal)
		signal.Notify(c, syscall.SIGINT) //监听指定信号
		_ = <-c                          //阻塞直至有信号传入
		fmt.Println("kvproxy stop")
	}
}
