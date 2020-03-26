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
		if len(logger) > 0 && logger[0] != nil {
			buf := make([]byte, 65535)
			l := runtime.Stack(buf, false)
			logger[0].Errorf(FormatFileLine("%s\n", fmt.Sprintf("%v: %s", r, buf[:l])))
		}
		if fn != nil {
			fn()
		}
	}
}

func Recover(logger ...golog.LoggerI) {
	if r := recover(); r != nil {
		if len(logger) > 0 && logger[0] != nil {
			buf := make([]byte, 65535)
			l := runtime.Stack(buf, false)
			logger[0].Errorf(FormatFileLine("%s\n", fmt.Sprintf("%v: %s", r, buf[:l])))
		}
	}
}

func ProtectCall(fn interface{}, args ...interface{}) (ret []interface{}, err error) {
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

	fnType := reflect.TypeOf(fn)

	in := make([]reflect.Value, len(args))
	for i, v := range args {
		if v == nil {
			in[i] = reflect.Zero(fnType.In(i))
		} else {
			in[i] = reflect.ValueOf(v)
		}
	}

	out := oriF.Call(in)
	if len(out) > 0 {
		ret = make([]interface{}, len(out))
		for i, v := range out {
			ret[i] = v.Interface()
		}
	}
	return
}
