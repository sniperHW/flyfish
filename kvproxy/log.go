package kvproxy

import (
	flyfish_logger "github.com/sniperHW/flyfish/logger"
	"github.com/sniperHW/kendynet"
)

var (
	logger kendynet.LoggerI
)

func InitLogger() {
	logConfig := GetConfig().Log
	logger = flyfish_logger.NewZapLogger("kvproxy.log", logConfig.LogDir, logConfig.LogLevel, logConfig.MaxLogfileSize, 14, logConfig.EnableLogStdout)
}
