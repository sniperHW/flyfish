package flykv

import (
	"github.com/sniperHW/flyfish/db/sql"
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
		net.InitLogger(l)
		raft.InitLogger(l)
		sql.InitLogger(l)
		logger.InitLogger(l)
	})
}

func GetLogger() *zap.Logger {
	return zapLogger
}

func GetSugar() *zap.SugaredLogger {
	return sugaredLogger
}
