package main

import (
	"flag"
	"fmt"
	"github.com/sniperHW/flyfish/logger"
	"github.com/sniperHW/flyfish/server/flypd"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"
)

func main() {

	id := flag.Int("id", 1, "node ID")
	pprof := flag.String("pprof", "localhost:9999", "pprof")
	config := flag.String("config", "flypd_config.toml", "config")
	service := flag.String("service", "localhost:8111", "ip:port")
	raftcluster := flag.String("raftcluster", "1@http://localhost:8111@voter", "raftcluster")
	join := flag.String("join", false, "set true if the node is new join node")

	go func() {
		http.ListenAndServe(*pprof, nil)
	}()

	flag.Parse()

	conf, err := flypd.LoadConfig(*config)

	if nil != err {
		fmt.Println(err)
		return
	}

	logname := fmt.Sprintf("flypd:%d.log", *id)

	flypd.InitLogger(logger.NewZapLogger(logname, conf.Log.LogDir, conf.Log.LogLevel, conf.Log.MaxLogfileSize, conf.Log.MaxAge, conf.Log.MaxBackups, conf.Log.EnableStdout))

	pd, err := flypd.NewPd(uint16(*id), *join, conf, *service, *raftcluster)

	if nil == err {
		c := make(chan os.Signal)
		signal.Notify(c, syscall.SIGINT) //监听指定信号
		_ = <-c                          //阻塞直至有信号传入
		pd.Stop()
		flypd.GetSugar().Infof("server stop")
	} else {
		flypd.GetSugar().Error(err)
	}
}
