package golog

import (
	"os"
	"fmt"
	"time"
	"io"
)

var (
	maxPathLen = 256
)

type OutputLogger struct {
	out      	io.Writer
	basePath 	string
	fileMax  	int            //文件分割字节数
	time     	time.Time      //底层文件的创建时间
	bytes       int            //累计写入文件的字节数量
	filename    string
}

var stdOutLogger *OutputLogger

//创建输出文件
func (self *OutputLogger) createOutputFile(now *time.Time) {
	year, month, day := now.Date()
	hour, min, sec   := now.Clock()
	dir := fmt.Sprintf("%s/%04d-%02d-%02d",self.basePath,year,month,day)
	if nil == os.MkdirAll(dir,os.ModePerm) {
		path := fmt.Sprintf("%s/%s[%d].%02d.%02d.%02d.log",dir,self.filename,os.Getpid(),hour,min,sec)
		mode := os.O_RDWR | os.O_CREATE | os.O_APPEND
		f , err := os.OpenFile(path, mode, 0666)
		if nil == err {
			self.out = f
			self.bytes = 0
			self.time  = *now
		}
	}
}

func (self *OutputLogger) checkOutputFile(now *time.Time) bool {
	if self.out == nil {
		return false
	}

	if self.out == os.Stdout {
		return true
	}

	if self.bytes >= self.fileMax {
		return false
	}

	fyear, fmonth, fday := self.time.Date()
	year, month, day := now.Date()
	if fyear != year || fmonth != month || fday != day {
		return false
	}

	return true
}

func (self *OutputLogger) Write(now *time.Time,buff []byte) {
	if false == self.checkOutputFile(now) {
		self.createOutputFile(now)
	}

	if self.out != nil {
		c , err := self.out.Write(buff)
		if nil == err {
			self.bytes += c
		}
	}
}

func NewOutputLogger(basePath string,filename string,fileMax int) *OutputLogger {
	return &OutputLogger{basePath:basePath,filename:filename,fileMax:fileMax}
}

func init() {
	stdOutLogger = &OutputLogger{out:os.Stdout}
}