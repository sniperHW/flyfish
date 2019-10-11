package util

import (
	"fmt"
	"github.com/sniperHW/kendynet/golog"
)

type defLogger struct {
}

func NewDefLogger() golog.LoggerI {
	return &defLogger{}
}

func (this *defLogger) Debugf(format string, v ...interface{}) {
	fmt.Printf(format, v...)
}
func (this *defLogger) Debugln(v ...interface{}) {
	fmt.Println(v...)
}
func (this *defLogger) Infof(format string, v ...interface{}) {
	fmt.Printf(format, v...)
}
func (this *defLogger) Infoln(v ...interface{}) {
	fmt.Println(v...)
}
func (this *defLogger) Warnf(format string, v ...interface{}) {
	fmt.Printf(format, v...)
}
func (this *defLogger) Warnln(v ...interface{}) {
	fmt.Println(v...)
}
func (this *defLogger) Errorf(format string, v ...interface{}) {
	fmt.Printf(format, v...)
}
func (this *defLogger) Errorln(v ...interface{}) {
	fmt.Println(v...)
}
func (this *defLogger) Fatalf(format string, v ...interface{}) {
	fmt.Printf(format, v...)
}
func (this *defLogger) Fatalln(v ...interface{}) {
	fmt.Println(v...)
}
func (this *defLogger) SetLevelByString(level string) {

}
