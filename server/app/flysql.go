package main

import (
	"flag"
	"fmt"
	"github.com/sniperHW/flyfish/logger"
	"github.com/sniperHW/flyfish/server/flysql"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	fmt.Println("here")

	pprof := flag.String("pprof", "localhost:8899", "pprof")
	config := flag.String("config", "flysql_config.toml", "config")
	service := flag.String("service", "localhost:8110", "service")

	flag.Parse()

	go func() {
		http.ListenAndServe(*pprof, nil)
	}()

	conf, err := flysql.LoadConfig(*config)

	if nil != err {
		fmt.Println(err)
		return
	}

	logname := fmt.Sprintf("flysql:%s.log", *service)

	flysql.InitLogger(logger.NewZapLogger(logname, conf.Log.LogDir, conf.Log.LogLevel, conf.Log.MaxLogfileSize, conf.Log.MaxAge, conf.Log.MaxBackups, conf.Log.EnableStdout))

	node, err := flysql.NewFlysql(*service, conf)
	if nil == err {
		c := make(chan os.Signal)
		signal.Notify(c, syscall.SIGINT) //监听指定信号
		_ = <-c                          //阻塞直至有信号传入
		node.Stop()
		flysql.GetSugar().Infof("server stop")
	} else {
		flysql.GetSugar().Error(err)
	}
}
