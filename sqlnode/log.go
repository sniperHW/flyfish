package sqlnode

import (
	"github.com/sniperHW/kendynet"
	"github.com/sniperHW/kendynet/golog"
)

var (
	globalLogger golog.LoggerI
)

func initLog() {
	logConfig := getConfig().Log
	if !logConfig.EnableLogStdout {
		golog.DisableStdOut()
	}

	fullname := "sqlserver"

	//fmt.Println(logConfig.LogDir, logConfig.LogPrefix)

	globalLogger = golog.New(fullname, golog.NewOutputLogger(logConfig.LogDir, logConfig.LogPrefix, logConfig.MaxLogfileSize))
	globalLogger.SetLevelByString(logConfig.LogLevel)
	kendynet.InitLogger(globalLogger)

	globalLogger.Infoln("init log.")
}

func getLogger() golog.LoggerI {
	return globalLogger
}
