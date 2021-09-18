package main

import (
	"flag"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"

	"github.com/sniperHW/flyfish/backend/db/sql"
	"github.com/sniperHW/flyfish/logger"
	"github.com/sniperHW/flyfish/server/flykv"
	"github.com/sniperHW/flyfish/server/flykv/metaLoader"
)

func main() {

	id := flag.Int("id", 1, "node ID")
	pprof := flag.String("pprof", "localhost:8899", "pprof")
	config := flag.String("config", "config.toml", "config")

	go func() {
		http.ListenAndServe(*pprof, nil)
	}()

	flag.Parse()

	conf, err := flykv.LoadConfig(*config)

	if nil != err {
		flykv.GetSugar().Error(err)
		return
	}

	logname := fmt.Sprintf("flykv:%d.log", *id)

	flykv.InitLogger(logger.NewZapLogger(logname, conf.Log.LogDir, conf.Log.LogLevel, 100, 14, true))

	dbConfig := conf.DBConfig

	meta, err := metaLoader.LoadDBMetaFromSqlCsv(conf.DBType, dbConfig.Host, dbConfig.Port, dbConfig.MetaDB, dbConfig.User, dbConfig.Password)

	if nil != err {
		flykv.GetSugar().Error(err)
		return
	}

	node := flykv.NewKvNode(*id, conf, meta, sql.CreateDbMeta, flykv.NewSqlDbBackend())

	err = node.Start()
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
