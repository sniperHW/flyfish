package util

import (
	"fmt"
	"github.com/sniperHW/kendynet/golog"
	"reflect"
	"runtime"
	"strings"
)

func FormatFileLine(format string, v ...interface{}) string {
	_, file, line, ok := runtime.Caller(1)
	if ok {
		s := fmt.Sprintf("[%s:%d]", file, line)
		return strings.Join([]string{s, fmt.Sprintf(format, v...)}, "")
	} else {
		return fmt.Sprintf(format, v...)
	}
}

func CallStack(maxStack int) string {
	var str string
	i := 1
	for {
		pc, file, line, ok := runtime.Caller(i)
		if !ok || i > maxStack {
			break
		}
		str += fmt.Sprintf("    stack: %d %v [file: %s] [func: %s] [line: %d]\n", i-1, ok, file, runtime.FuncForPC(pc).Name(), line)
		i++
	}
	return str
}

func RecoverAndCall(fn func(), logger ...golog.LoggerI) {
	if r := recover(); r != nil {
		var logger_ golog.LoggerI
		if len(logger) > 0 {
			logger_ = logger[0]
		}
		if nil != logger_ {
			buf := make([]byte, 65535)
			l := runtime.Stack(buf, false)
			logger_.Errorf(FormatFileLine("%s\n", fmt.Sprintf("%v: %s", r, buf[:l])))
		}
		if fn != nil {
			fn()
		}
	}
}

func Recover(logger ...golog.LoggerI) {
	if r := recover(); r != nil {
		var logger_ golog.LoggerI
		if len(logger) > 0 {
			logger_ = logger[0]
		}
		if nil != logger_ {
			buf := make([]byte, 65535)
			l := runtime.Stack(buf, false)
			logger_.Errorf(FormatFileLine("%s\n", fmt.Sprintf("%v: %s", r, buf[:l])))
		}
	}
}

func PCall(fn interface{}, args ...interface{}) (err error, ret []interface{}) {
	defer func() {
		if r := recover(); r != nil {
			buf := make([]byte, 65535)
			l := runtime.Stack(buf, false)
			err = fmt.Errorf(fmt.Sprintf("%v: %s", r, buf[:l]))

		}
	}()

	oriF := reflect.ValueOf(fn)

	if oriF.Kind() != reflect.Func {
		err = fmt.Errorf("fn is not Func")
		return
	}

	in := []reflect.Value{}
	for _, v := range args {
		in = append(in, reflect.ValueOf(v))
	}

	out := oriF.Call(in)
	if len(out) > 0 {
		ret = make([]interface{}, len(out))[0:0]
		for _, v := range out {
			ret = append(ret, v.Interface())
		}
	}
	return
}
