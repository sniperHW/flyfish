package kvnode

import (
	"github.com/sniperHW/flyfish/backend/db/sql"
	"github.com/sniperHW/flyfish/pkg/net"
	"github.com/sniperHW/flyfish/pkg/raft"
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
		raft.InitLogger(logger)
		sql.InitLogger(logger)
	})
}

func GetLogger() *zap.Logger {
	return zapLogger
}

func GetSugar() *zap.SugaredLogger {
	return sugaredLogger
}
