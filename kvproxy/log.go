package kvproxy

import (
	"fmt"
	"github.com/sniperHW/kendynet"
	"github.com/sniperHW/kendynet/golog"
)

var (
	logger *golog.Logger
)

func InitLogger() {
	logConfig := GetConfig().Log
	if !logConfig.EnableLogStdout {
		golog.DisableStdOut()
	}
	fullname := "kvproxy"

	fmt.Println(logConfig.LogDir, logConfig.LogPrefix)

	logger = golog.New(fullname, golog.NewOutputLogger(logConfig.LogDir, logConfig.LogPrefix, logConfig.MaxLogfileSize))
	logger.SetLevelByString(logConfig.LogLevel)
	kendynet.InitLogger(logger)
	logger.Infof("%s logger init", fullname)

}
