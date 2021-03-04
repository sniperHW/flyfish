package kvnode

import (
	"fmt"
	"github.com/sniperHW/flyfish/conf"
	flyfish_logger "github.com/sniperHW/flyfish/logger"
)

func InitLogger() {
	logConfig := conf.GetConfig().Log

	name := fmt.Sprintf("kvnode_%s:%d.log", conf.GetConfig().ServiceHost, conf.GetConfig().ServicePort)

	flyfish_logger.InitLogger(flyfish_logger.NewZapLogger(name, logConfig.LogDir, logConfig.LogLevel, logConfig.MaxLogfileSize, 14, logConfig.EnableLogStdout))
}

func UpdateLogConfig() {

}
