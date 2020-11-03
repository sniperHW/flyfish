package kendynet

import (
	//"fmt"
	"github.com/sniperHW/kendynet/golog"
)

var logger golog.LoggerI

func InitLogger(l golog.LoggerI) {
	logger = l
}

func GetLogger() golog.LoggerI {
	if nil == logger {
		return &EmptyLogger{}
	}
	return logger
}

type EmptyLogger struct {
}

func (this *EmptyLogger) Debugf(format string, v ...interface{}) {}
func (this *EmptyLogger) Debugln(v ...interface{})               {}
func (this *EmptyLogger) Infof(format string, v ...interface{})  {}
func (this *EmptyLogger) Infoln(v ...interface{})                {}
func (this *EmptyLogger) Warnf(format string, v ...interface{})  {}
func (this *EmptyLogger) Warnln(v ...interface{})                {}
func (this *EmptyLogger) Errorf(format string, v ...interface{}) {}
func (this *EmptyLogger) Errorln(v ...interface{})               {}
func (this *EmptyLogger) Fatalf(format string, v ...interface{}) {}
func (this *EmptyLogger) Fatalln(v ...interface{})               {}
func (this *EmptyLogger) SetLevelByString(level string)          {}

/*
func Debugf(format string, v ...interface{}) {
	if nil != logger {
		logger.Debugf(format, v...)
	}
}

func Debugln(v ...interface{}) {
	if nil != logger {
		logger.Debugln(v...)
	}
}

func Infof(format string, v ...interface{}) {
	if nil != logger {
		logger.Infof(format, v...)
	}
}

func Infoln(v ...interface{}) {
	if nil != logger {
		logger.Infoln(v...)
	}
}

func Warnf(format string, v ...interface{}) {
	if nil != logger {
		logger.Warnf(format, v...)
	}
}

func Warnln(v ...interface{}) {
	if nil != logger {
		logger.Warnln(v...)
	}
}

func Errorf(format string, v ...interface{}) {
	if nil != logger {
		logger.Errorf(format, v...)
	}
}

func Errorln(v ...interface{}) {
	if nil != logger {
		logger.Errorln(v...)
	}
}

func Fatalf(format string, v ...interface{}) {
	if nil != logger {
		logger.Fatalf(format, v...)
	}
}

func Fatalln(v ...interface{}) {
	if nil != logger {
		logger.Fatalln(v...)
	}
}
*/
