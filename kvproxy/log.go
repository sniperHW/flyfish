package kvproxy

import (
	flyfish_logger "github.com/sniperHW/flyfish/logger"
)

func InitLogger() {
	logConfig := GetConfig().Log
	flyfish_logger.InitLogger(flyfish_logger.NewZapLogger("kvproxy.log", logConfig.LogDir, logConfig.LogLevel, logConfig.MaxLogfileSize, 14, logConfig.EnableLogStdout))
}
