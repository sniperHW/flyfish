package flyfish

import (
	"flyfish/conf"

	"github.com/sniperHW/kendynet"
	"github.com/sniperHW/kendynet/golog"
)

var (
	logger *golog.Logger
)

func InitLogger() {
	if !conf.EnableLogStdout {
		golog.DisableStdOut()
	}
	fullname := "flyfish"
	logger = golog.New(fullname, golog.NewOutputLogger(conf.LogDir, conf.LogPrefix, conf.MaxLogfileSize))
	logger.SetLevelByString(conf.LogLevel)
	kendynet.InitLogger(logger)
	logger.Infof("%s logger init", fullname)

}

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
