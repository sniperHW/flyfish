package main

import (
	"flyfish"
	"flyfish/conf"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	flyfish.Must(nil, conf.LoadConfig(os.Args[1]))
	config := conf.GetConfig()

	flyfish.InitLogger()

	if !flyfish.LoadTableConfig() {
		fmt.Println("InitTableConfig failed")
		return
	}

	flyfish.InitProcessUnit()
	flyfish.RedisInit()
	flyfish.Recover()

	go func() {
		http.ListenAndServe("0.0.0.0:8899", nil)
	}()

	err := flyfish.StartTcpServer("tcp", fmt.Sprintf("%s:%d", config.ServiceHost, config.ServicePort))
	if nil == err {
		fmt.Println("flyfish start:", fmt.Sprintf("%s:%d", config.ServiceHost, config.ServicePort))
		c := make(chan os.Signal)
		signal.Notify(c, syscall.SIGINT) //监听指定信号
		_ = <-c                          //阻塞直至有信号传入
		flyfish.Stop()
		fmt.Println("server stop")
	} else {
		fmt.Println(err)
	}
}
