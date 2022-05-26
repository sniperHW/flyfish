package main

import (
	"flag"
	"fmt"
	"github.com/sniperHW/flyfish/logger"
	"github.com/sniperHW/flyfish/server/flykv"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"
)

func main() {

	id := flag.Int("id", 1, "node ID")
	pprof := flag.String("pprof", "localhost:8899", "pprof")
	config := flag.String("config", "flykv_config.toml", "config")
	join := flag.Bool("join", false, "set true if the node is new join node")

	flag.Parse()

	go func() {
		http.ListenAndServe(*pprof, nil)
	}()

	conf, err := flykv.LoadConfig(*config)

	if nil != err {
		fmt.Println(err)
		return
	}

	logname := fmt.Sprintf("flykv:%d.log", *id)

	flykv.InitLogger(logger.NewZapLogger(logname, conf.Log.LogDir, conf.Log.LogLevel, conf.Log.MaxLogfileSize, conf.Log.MaxAge, conf.Log.MaxBackups, conf.Log.EnableStdout))

	node, err := flykv.NewKvNode(uint16(*id), *join, conf, flykv.NewSqlDB())
	if nil == err {
		c := make(chan os.Signal)
		signal.Notify(c, syscall.SIGINT) //监听指定信号
		_ = <-c                          //阻塞直至有信号传入
		node.Stop()
		flykv.GetSugar().Infof("server stop")
	} else {
		flykv.GetSugar().Error(err)
	}
}
