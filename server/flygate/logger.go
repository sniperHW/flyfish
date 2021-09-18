package flygate

import (
	"github.com/sniperHW/flyfish/pkg/net"
	"go.uber.org/zap"
	"sync"
)

var initOnce sync.Once
var zapLogger *zap.Logger
var sugaredLogger *zap.SugaredLogger

func InitLogger(logger *zap.Logger) {
	initOnce.Do(func() {
		zapLogger = logger
		sugaredLogger = zapLogger.Sugar()
		net.InitLogger(logger)
	})
}

func GetLogger() *zap.Logger {
	return zapLogger
}

func GetSugar() *zap.SugaredLogger {
	return sugaredLogger
}
