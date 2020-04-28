package kvnode

import (
	"fmt"
	"github.com/sniperHW/flyfish/conf"
	"github.com/sniperHW/kendynet"
	"github.com/sniperHW/kendynet/golog"
)

var (
	logger *golog.Logger
)

func InitLogger() {
	logConfig := conf.GetConfig().Log
	if !logConfig.EnableLogStdout {
		golog.DisableStdOut()
	}
	fullname := "kvserver"

	fmt.Println(logConfig.LogDir, logConfig.LogPrefix)

	logger = golog.New(fullname, golog.NewOutputLogger(logConfig.LogDir, logConfig.LogPrefix, logConfig.MaxLogfileSize))
	logger.SetLevelByString(logConfig.LogLevel)
	kendynet.InitLogger(logger)
	logger.Infof("%s logger init", fullname)

}

func UpdateLogConfig() {
	logConfig := conf.GetConfig().Log
	if logConfig.EnableLogStdout {
		golog.EnableStdOut()
	} else {
		golog.DisableStdOut()
	}
	logger.SetLevelByString(logConfig.LogLevel)
}
