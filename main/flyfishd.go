package main

import (
	"flyfish"
	"flyfish/conf"
	"fmt"
	"github.com/go-ini/ini"
	//"github.com/sniperHW/kendynet/golog"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"
)

func main() {

	if len(os.Args) > 1 {
		cfg, err := ini.LooseLoad(os.Args[1])
		if err != nil {
			return
		}

		sec := cfg.Section("Config")
		if nil == sec {
			return
		}
		conf.ParseConfig(sec)
	}

	if !flyfish.InitTableConfig() {
		fmt.Println("InitTableConfig failed")
		return
	}

	flyfish.InitLogger()

	flyfish.InitCacheKey()
	flyfish.RedisInit(conf.RedisHost, conf.RedisPort, conf.RedisPassword)
	flyfish.SQLInit(conf.PgsqlHost, conf.PgsqlPort, conf.PgsqlDataBase, conf.PgsqlUser, conf.PgsqlPassword)
	flyfish.Recover()

	go func() {
		http.ListenAndServe("0.0.0.0:8899", nil)
	}()

	err := flyfish.StartTcpServer("tcp", fmt.Sprintf("%s:%d", conf.ServiceHost, conf.ServicePort))
	if nil == err {
		fmt.Println("flyfish start:", fmt.Sprintf("%s:%d", conf.ServiceHost, conf.ServicePort))
		c := make(chan os.Signal)
		signal.Notify(c, syscall.SIGINT) //监听指定信号
		_ = <-c                          //阻塞直至有信号传入
		flyfish.Stop()
		fmt.Println("server stop")
	} else {
		fmt.Println(err)
	}
}
