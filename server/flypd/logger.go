package flypd

import (
	"github.com/sniperHW/flyfish/logger"
	"github.com/sniperHW/flyfish/pkg/net"
	"github.com/sniperHW/flyfish/pkg/raft"
	"go.uber.org/zap"
	"sync"
)

var initOnce sync.Once
var zapLogger *zap.Logger
var sugaredLogger *zap.SugaredLogger

func InitLogger(l *zap.Logger) {
	initOnce.Do(func() {
		zapLogger = l
		sugaredLogger = zapLogger.Sugar()
		raft.InitLogger(l)
		net.InitLogger(l)
		logger.InitLogger(l)
	})
}

func GetLogger() *zap.Logger {
	return zapLogger
}

func GetSugar() *zap.SugaredLogger {
	return sugaredLogger
}
