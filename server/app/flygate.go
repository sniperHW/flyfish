package main

import (
	"flag"
	"fmt"
	"github.com/sniperHW/flyfish/logger"
	"github.com/sniperHW/flyfish/server/flygate"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"
)

func main() {

	pprof := flag.String("pprof", "localhost:8999", "pprof")
	config := flag.String("config", "flygate_config.toml", "config")
	service := flag.String("service", "localhost:8110", "ip:port")

	go func() {
		http.ListenAndServe(*pprof, nil)
	}()

	flag.Parse()

	conf, err := flygate.LoadConfig(*config)

	if nil != err {
		fmt.Println(err)
		return
	}

	logname := fmt.Sprintf("flygate_%s.log", *service)

	flygate.InitLogger(logger.NewZapLogger(logname, conf.Log.LogDir, conf.Log.LogLevel, conf.Log.MaxLogfileSize, conf.Log.MaxAge, conf.Log.MaxBackups, conf.Log.EnableStdout))

	gate := flygate.NewFlyGate(conf, *service)

	err = gate.Start()
	if nil == err {
		c := make(chan os.Signal)
		signal.Notify(c, syscall.SIGINT) //监听指定信号
		_ = <-c                          //阻塞直至有信号传入
		gate.Stop()
		flygate.GetSugar().Infof("server stop")
	} else {
		flygate.GetSugar().Error(err)
	}
}
