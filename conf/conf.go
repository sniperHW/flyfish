package conf

import(
	"github.com/go-ini/ini"
	"strconv"
	"fmt"
)

var RedisProcessPoolSize    = int(5)
var SqlLoadPoolSize         = int(5)
var SqlUpdatePoolSize       = int(10)
var RedisPipelineSize       = int(50)
var SqlLoadPipeLineSize     = int(200)
var SqlUpdatePipeLineSize   = int(1000)
var SqlUpdateEventQueueSize = int(100000)
var SqlLoadEventQueueSize   = int(10000)
var RedisEventQueueSize     = int(50000)
var WriteBackEventQueueSize = int(100000)
var MainEventQueueSize      = int(100000)
var MaxPacketSize           = uint64(1024*1024*4)
var WriteBackDelay          = int64(5)
var MaxUpdateStringSize     = int(1024*1024*4)
var StrInitCap              = int(1024*1024)
var ServiceHost             = "127.0.0.1"
var ServicePort             = 10012
var RedisHost               = "127.0.0.1"
var RedisPort               = 6379
var RedisPassword           = ""
var PgsqlHost               = "127.0.0.1"
var PgsqlPort               = 5432
var PgsqlUser               = "sniper"
var PgsqlPassword           = "802802"
var PgsqlDataBase           = "test"

var ConfDbHost              = "127.0.0.1"
var ConfDbPort              = 5432
var ConfDbUser              = "sniper"
var ConfDbPassword          = "802802"
var ConfDataBase            = "test"


var MaxLogfileSize          = int(1024*1024*100)
var LogDir                  = "log"
var LogPrefix               = "flyfish"
var LogLevel                = "info"
var EnableLogStdout         = false

var parser map[string]func(string)

func ParseConfig(sec *ini.Section) {
	keys := sec.Keys()
	for _,v := range(keys) {
		f,ok := parser[v.Name()]
		if ok {
			f(v.Value())
		}	
	}
}

func redisProcessPoolSize(v string) {
	i, err := strconv.ParseInt(v, 10, 32)
	if nil == err {
		RedisProcessPoolSize = int(i)
	}
}

func sqlLoadPoolSize(v string) {
	i, err := strconv.ParseInt(v, 10, 32)
	if nil == err {
		SqlLoadPoolSize = int(i)
	}	
}

func sqlUpdatePoolSize(v string) {
	i, err := strconv.ParseInt(v, 10, 32)
	if nil == err {
		SqlUpdatePoolSize = int(i)
	}	
}

func redisPipelineSize(v string) {
	i, err := strconv.ParseInt(v, 10, 32)
	if nil == err {
		RedisPipelineSize = int(i)
	}	
}

func sqlLoadPipeLineSize(v string) {
	i, err := strconv.ParseInt(v, 10, 32)
	if nil == err {
		SqlLoadPipeLineSize = int(i)
	}	
}

func sqlUpdatePipeLineSize(v string) {
	i, err := strconv.ParseInt(v, 10, 32)
	if nil == err {
		SqlUpdatePipeLineSize = int(i)
	}	
}

func sqlUpdateEventQueueSize(v string) {
	i, err := strconv.ParseInt(v, 10, 32)
	if nil == err {
		SqlUpdateEventQueueSize = int(i)
	}	
}

func sqlLoadEventQueueSize(v string) {
	i, err := strconv.ParseInt(v, 10, 32)
	if nil == err {
		SqlLoadEventQueueSize = int(i)
	}	
}

func redisEventQueueSize(v string) {
	i, err := strconv.ParseInt(v, 10, 32)
	if nil == err {
		RedisEventQueueSize = int(i)
	}	
}

func writeBackEventQueueSize(v string) {
	i, err := strconv.ParseInt(v, 10, 32)
	if nil == err {
		WriteBackEventQueueSize = int(i)
	}	
}

func mainEventQueueSize(v string) {
	i, err := strconv.ParseInt(v, 10, 32)
	if nil == err {
		MainEventQueueSize = int(i)
	}	
}

func parseByteCount(v string) (int64,error) {
	if len(v) > 2 {
		unit  := v[len(v)-2:]
		value := v[:len(v)-2]
		if unit == "mb" {
			i, err := strconv.ParseInt(value, 10, 32)
			if nil != err {
				return 0,err
			}
			return i*1024*1024,nil
		} else if unit == "kb" {
			i, err := strconv.ParseInt(value, 10, 32)
			if nil != err {
				return 0,err
			}
			return i*1024,nil
		} else {
			return 0,fmt.Errorf("invaild bytecount")
		}
	}
	return 0,fmt.Errorf("invaild bytecount")
}

func maxPacketSize(v string) {
	i, err := parseByteCount(v)
	if nil == err {
		MaxPacketSize = uint64(i)
	}	
}

func writeBackDelay(v string) {
	i, err := strconv.ParseInt(v, 10, 32)
	if nil == err {
		WriteBackDelay = i
	}	
}

func maxUpdateStringSize(v string) {
	i, err := parseByteCount(v)
	if nil == err {
		MaxUpdateStringSize = int(i)
	}	
}

func strInitCap(v string) {
	i, err := parseByteCount(v)
	if nil == err {
		StrInitCap = int(i)
	}	
}

func serviceHost(v string) {
	ServiceHost = v
}

func servicePort(v string) {
	i, err := strconv.ParseInt(v, 10, 32)
	if nil == err {
		ServicePort = int(i)
	}	
}

func redisHost(v string) {
	RedisHost = v
}

func redisPort(v string) {
	i, err := strconv.ParseInt(v, 10, 32)
	if nil == err {
		RedisPort = int(i)
	}	
}

func redisPassword(v string) {
	RedisPassword = v
}


func confDbHost(v string) {
	ConfDbHost = v
}

func confDbPort(v string) {
	i, err := strconv.ParseInt(v, 10, 32)
	if nil == err {
		ConfDbPort = int(i)
	}	
}

func confDbUser(v string) {
	ConfDbUser = v
}

func confDbPassword(v string) {
	ConfDbPassword = v
}

func confDataBase(v string) {
	ConfDataBase = v
}



func pgsqlHost(v string) {
	PgsqlHost = v
}

func pgsqlPort(v string) {
	i, err := strconv.ParseInt(v, 10, 32)
	if nil == err {
		PgsqlPort = int(i)
	}	
}

func pgsqlUser(v string) {

	PgsqlUser = v
	
}

func pgsqlPassword(v string) {
	PgsqlPassword = v
}

func pgsqlDataBase(v string) {
	PgsqlDataBase = v
}

func maxLogfileSize(v string) {
	i, err := parseByteCount(v)
	if nil == err {
		MaxLogfileSize = int(i)
	}	
}

func logDir(v string) {
	LogDir = v
}

func logPrefix(v string) {
	LogPrefix = v
}

func logLevel(v string) {
	LogLevel = v
}

func enableLogStdout(v string) {	
	if v == "false" {
		EnableLogStdout = false
	} else if v == "true" {
		EnableLogStdout = true
	}
}

func init() {
	parser = map[string]func(string){}
	parser["RedisProcessPoolSize"] = redisProcessPoolSize
	parser["SqlLoadPoolSize"] = sqlLoadPoolSize	
	parser["SqlUpdatePoolSize"] = sqlUpdatePoolSize
	parser["RedisPipelineSize"] = redisPipelineSize
	parser["SqlLoadPipeLineSize"] = sqlLoadPipeLineSize

	parser["SqlUpdatePipeLineSize"] = sqlUpdatePipeLineSize
	parser["SqlUpdateEventQueueSize"] = sqlUpdateEventQueueSize	
	parser["SqlLoadEventQueueSize"] = sqlLoadEventQueueSize
	parser["RedisEventQueueSize"] = redisEventQueueSize
	parser["WriteBackEventQueueSize"] = writeBackEventQueueSize

	parser["MainEventQueueSize"] = mainEventQueueSize
	parser["MaxPacketSize"] = maxPacketSize	
	parser["WriteBackDelay"] = writeBackDelay
	parser["MaxUpdateStringSize"] = maxUpdateStringSize
	parser["StrInitCap"] = strInitCap

	parser["ServiceHost"] = serviceHost
	parser["ServicePort"] = servicePort	
	parser["RedisHost"] = redisHost
	parser["RedisPort"] = redisPort
	parser["RedisPassword"] = redisPassword

	parser["PgsqlHost"] = pgsqlHost
	parser["PgsqlPort"] = pgsqlPort	
	parser["PgsqlUser"] = pgsqlUser
	parser["PgsqlPassword"] = pgsqlPassword
	parser["PgsqlDataBase"] = pgsqlDataBase

	parser["ConfDbHost"] = confDbHost
	parser["ConfDbPort"] = confDbPort	
	parser["ConfDbUser"] = confDbUser
	parser["ConfDbPassword"] = confDbPassword
	parser["ConfDataBase"] = confDataBase

	parser["MaxLogfileSize"] = maxLogfileSize
	parser["LogDir"] = logDir	
	parser["LogPrefix"] = logPrefix
	parser["LogLevel"] = logLevel
	parser["EnableLogStdout"] = enableLogStdout


}
