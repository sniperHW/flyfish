package kendynet

import (
	"fmt"
)

var logger LoggerI

type LoggerI interface {
	Debugf(format string, v ...interface{})
	Debug(v ...interface{})
	Infof(format string, v ...interface{})
	Info(v ...interface{})
	Warnf(format string, v ...interface{})
	Warn(v ...interface{})
	Errorf(format string, v ...interface{})
	Error(v ...interface{})
	Fatalf(format string, v ...interface{})
	Fatal(v ...interface{})
}

func InitLogger(l LoggerI) {
	logger = l
}

func GetLogger() LoggerI {
	if nil == logger {
		return &EmptyLogger{}
	}
	return logger
}

type EmptyLogger struct {
}

func (this *EmptyLogger) Debugf(format string, v ...interface{}) {
	fmt.Printf(format, v...)
}

func (this *EmptyLogger) Debug(v ...interface{}) {
	fmt.Println(v...)
}
func (this *EmptyLogger) Infof(format string, v ...interface{}) {
	fmt.Printf(format, v...)
}
func (this *EmptyLogger) Info(v ...interface{}) {
	fmt.Println(v...)
}
func (this *EmptyLogger) Warnf(format string, v ...interface{}) {
	fmt.Printf(format, v...)
}
func (this *EmptyLogger) Warn(v ...interface{}) {
	fmt.Println(v...)
}
func (this *EmptyLogger) Errorf(format string, v ...interface{}) {
	fmt.Printf(format, v...)
}
func (this *EmptyLogger) Error(v ...interface{}) {
	fmt.Println(v...)
}
func (this *EmptyLogger) Fatalf(format string, v ...interface{}) {
	fmt.Printf(format, v...)
}
func (this *EmptyLogger) Fatal(v ...interface{}) {
	fmt.Println(v...)
}
