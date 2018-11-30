package util

import (
	"runtime"
	"strings"
	"fmt"
)

func FormatFileLine(format string,v ...interface{}) string {
	_, file, line, ok := runtime.Caller(1)
	if ok {
		s := fmt.Sprintf("[%s:%d]",file,line)
		return strings.Join([]string{s,fmt.Sprintf(format,v...)},"")
	} else {
		return fmt.Sprintf(format,v...)
	}
}