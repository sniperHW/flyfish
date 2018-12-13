package main

import(
	"flyfish"
	"fmt"
	"github.com/sniperHW/kendynet/golog"
	_ "net/http/pprof"
	"net/http"
	"github.com/sniperHW/kendynet"
	"os"
	"os/signal"
	"syscall"
	"flyfish/conf"
	"github.com/go-ini/ini"
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
	
	if !conf.EnableLogStdout {
		golog.DisableStdOut()
	}

	outLogger := golog.NewOutputLogger(conf.LogDir, conf.LogPrefix, conf.MaxLogfileSize)
	flyfish.InitLogger(outLogger,golog.Str2loglevel(conf.LogLevel))
	kendynet.InitLogger(outLogger,conf.LogPrefix)

	metas := []string{
		"users1@age:int:0,phone:string:123,name:string:haha",
		"counter@c:int:0",
	}

	if !flyfish.InitMeta(metas) {
		fmt.Println("InitMeta failed")
		return
	}
	flyfish.RedisInit(conf.RedisHost,conf.RedisPort,conf.RedisPassword)
	flyfish.SQLInit(conf.PgsqlHost, conf.PgsqlPort, conf.PgsqlDataBase, conf.PgsqlUser, conf.PgsqlPassword)

	go func() {
    	http.ListenAndServe("0.0.0.0:8899", nil)
	}()

	err := flyfish.StartTcpServer("tcp",fmt.Sprintf("%s:%d",conf.ServiceHost,conf.ServicePort))
	if nil == err {
		fmt.Println("flyfish start ok")
		c := make(chan os.Signal) 
		signal.Notify(c, syscall.SIGINT) //监听指定信号
		_ = <-c //阻塞直至有信号传入
		flyfish.Stop()
		fmt.Println("server stop")
	} else {
		fmt.Println(err)
	}
}