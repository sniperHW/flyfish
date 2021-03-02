package kvnode

import (
	"github.com/sniperHW/flyfish/conf"
	flyfish_logger "github.com/sniperHW/flyfish/logger"
	"github.com/sniperHW/kendynet"
)

var (
	logger kendynet.LoggerI
)

func InitLogger() {
	logConfig := conf.GetConfig().Log
	logger = flyfish_logger.NewZapLogger("kvnode.log", logConfig.LogDir, logConfig.LogLevel, logConfig.MaxLogfileSize, 14, logConfig.EnableLogStdout)
}

func UpdateLogConfig() {

}
