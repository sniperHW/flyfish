package logger

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
	"os"
	"sync"
	"time"
)

type stdoutWriteSyncer struct {
}

func (s stdoutWriteSyncer) Write(p []byte) (n int, err error) {
	n = len(p)
	os.Stdout.Write(p)
	return
}

func (s stdoutWriteSyncer) Sync() error {
	return nil
}

var levelMap = map[string]zapcore.Level{

	"debug": zapcore.DebugLevel,

	"info": zapcore.InfoLevel,

	"warn": zapcore.WarnLevel,

	"error": zapcore.ErrorLevel,

	"dpanic": zapcore.DPanicLevel,

	"panic": zapcore.PanicLevel,

	"fatal": zapcore.FatalLevel,
}

func getLoggerLevel(lvl string) zapcore.Level {
	if level, ok := levelMap[lvl]; ok {
		return level
	}
	return zapcore.InfoLevel
}

func TimeEncoder(t time.Time, enc zapcore.PrimitiveArrayEncoder) {
	enc.AppendString(t.Format("2006-01-02 15:04:05.000"))
}

func NewZapLogger(name string, path string, level string, maxLogfileSize int, maxAge int, enableLogStdout bool) *zap.Logger {

	syncWriter := zapcore.AddSync(&lumberjack.Logger{
		Filename:  path + "/" + name,
		MaxSize:   maxLogfileSize,
		MaxAge:    maxAge,
		LocalTime: true,
		//Compress: true,
	})

	var w zapcore.WriteSyncer

	encoder := zap.NewProductionEncoderConfig()

	encoder.EncodeTime = TimeEncoder

	if enableLogStdout {
		//encoder.EncodeLevel = zapcore.CapitalColorLevelEncoder
		w = zap.CombineWriteSyncers(syncWriter, stdoutWriteSyncer{})
	} else {
		w = zap.CombineWriteSyncers(syncWriter)
	}
	/*NewConsoleEncoder*/
	/*NewJSONEncoder*/

	core := zapcore.NewCore(zapcore.NewConsoleEncoder(encoder), w, zap.NewAtomicLevelAt(getLoggerLevel(level)))

	return zap.New(core, zap.AddCaller())
}

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
