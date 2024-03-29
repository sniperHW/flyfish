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
	cluster := flag.Int("cluster", 1, "cluster id")
	pprof := flag.String("pprof", "localhost:9999", "pprof")
	config := flag.String("config", "flypd_config.toml", "config")
	raftcluster := flag.String("raftcluster", "1@1@http://localhost:18111@localhost:8111@voter", "raftcluster")
	join := flag.Bool("join", false, "set true if the node is new join node")

	flag.Parse()

	go func() {
		http.ListenAndServe(*pprof, nil)
	}()

	conf, err := flypd.LoadConfig(*config)

	if nil != err {
		fmt.Println(err)
		return
	}

	logname := fmt.Sprintf("flypd:%d.log", *id)

	flypd.InitLogger(logger.NewZapLogger(logname, conf.Log.LogDir, conf.Log.LogLevel, conf.Log.MaxLogfileSize, conf.Log.MaxAge, conf.Log.MaxBackups, conf.Log.EnableStdout))

	pd, err := flypd.NewPd(uint16(*id), int(*cluster), *join, conf, *raftcluster)

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
