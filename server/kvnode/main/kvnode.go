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
	kvnode "github.com/sniperHW/flyfish/server/kvnode"
	"github.com/sniperHW/flyfish/server/kvnode/metaLoader"
)

func main() {

	id := flag.Int("id", 1, "node ID")
	pprof := flag.String("pprof", "localhost:8899", "pprof")
	config := flag.String("config", "config.toml", "config")

	go func() {
		http.ListenAndServe(*pprof, nil)
	}()

	flag.Parse()

	conf, err := kvnode.LoadConfig(*config)

	if nil != err {
		kvnode.GetSugar().Error(err)
		return
	}

	logname := fmt.Sprintf("kvnode:%d.log", *id)

	kvnode.InitLogger(logger.NewZapLogger(logname, conf.Log.LogDir, conf.Log.LogLevel, 100, 14, true))

	dbConfig := conf.DBConfig

	meta, err := metaLoader.LoadDBMetaFromSqlCsv(dbConfig.SqlType, dbConfig.ConfDbHost, dbConfig.ConfDbPort, dbConfig.ConfDataBase, dbConfig.ConfDbUser, dbConfig.ConfDbPassword)

	if nil != err {
		kvnode.GetSugar().Error(err)
		return
	}

	node := kvnode.NewKvNode(*id, conf, meta, sql.CreateDbMeta, kvnode.NewSqlDbBackend())

	err = node.Start()
	if nil == err {
		c := make(chan os.Signal)
		signal.Notify(c, syscall.SIGINT) //监听指定信号
		_ = <-c                          //阻塞直至有信号传入
		node.Stop()
		kvnode.GetSugar().Infof("server stop")
	} else {
		kvnode.GetSugar().Error(err)
	}
}
