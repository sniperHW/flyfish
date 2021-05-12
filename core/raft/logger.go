package raft

import (
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
	})
}

func GetLogger() *zap.Logger {
	return zapLogger
}

func GetSugar() *zap.SugaredLogger {
	return sugaredLogger
}

type raftLogger struct {
	loger *zap.Logger
	sugar *zap.SugaredLogger
}

func (l raftLogger) Debug(v ...interface{}) {
	l.sugar.Debug(v...)
}

func (l raftLogger) Debugf(format string, v ...interface{}) {
	l.sugar.Debugf(format, v...)
}

func (l raftLogger) Error(v ...interface{}) {
	l.sugar.Error(v...)
}

func (l raftLogger) Errorf(format string, v ...interface{}) {
	l.sugar.Errorf(format, v...)
}

func (l raftLogger) Info(v ...interface{}) {
	l.sugar.Info(v...)
}

func (l raftLogger) Infof(format string, v ...interface{}) {
	l.sugar.Infof(format, v...)
}

func (l raftLogger) Warning(v ...interface{}) {
	l.sugar.Warn(v...)
}

func (l raftLogger) Warningf(format string, v ...interface{}) {
	l.sugar.Warnf(format, v...)
}

func (l raftLogger) Fatal(v ...interface{}) {
	l.sugar.Fatal(v...)
}

func (l raftLogger) Fatalf(format string, v ...interface{}) {
	l.sugar.Fatalf(format, v...)
}

func (l raftLogger) Panic(v ...interface{}) {
	l.sugar.Panic(v...)
}

func (l raftLogger) Panicf(format string, v ...interface{}) {
	l.sugar.Panicf(format, v...)
}
