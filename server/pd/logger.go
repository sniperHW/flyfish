package pd

import (
	"github.com/sniperHW/flyfish/core/raft"
	"github.com/sniperHW/flyfish/net"
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
		raft.InitLogger(logger)
		net.InitLogger(logger)
	})
}

func GetLogger() *zap.Logger {
	return zapLogger
}

func GetSugar() *zap.SugaredLogger {
	return sugaredLogger
}
