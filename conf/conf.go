package conf

import (
	"github.com/BurntSushi/toml"
	"sync/atomic"
	"unsafe"
)

const (
	MaxPacketSize = 4 * 1024 * 1024 // 4mb
)

var (
	defConfig *Config
)

func LoadConfig(path string) error {
	config := &Config{}
	_, err := toml.DecodeFile(path, config)
	if nil != err {
		return err
	} else {
		atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&defConfig)), unsafe.Pointer(config))
		return nil
	}
}

func GetConfig() *Config {
	return (*Config)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&defConfig))))
}

type Config struct {
	CacheGroupSize       int
	MaxCachePerGroupSize int
	RedisProcessPoolSize int
	RedisPipelineSize    int
	SqlLoadPipeLineSize  int
	SqlUpdateQueueSize   int
	SqlLoadQueueSize     int
	RedisQueueSize       int
	//StrInitCap           int
	ServiceHost          string
	ServicePort          int
	BackDir              string
	BackFile             string
	ReplyBusyOnQueueFull bool
	Compress             bool
	CacheType            string

	Redis struct {
		RedisHost     string
		RedisPort     int
		RedisPassword string
	}

	DBConfig struct {
		SqlType string

		DbHost     string
		DbPort     int
		DbUser     string
		DbPassword string
		DbDataBase string

		ConfDbHost     string
		ConfDbPort     int
		ConfDbUser     string
		ConfDbPassword string
		ConfDataBase   string
	}

	Log struct {
		MaxLogfileSize  int
		LogDir          string
		LogPrefix       string
		LogLevel        string
		EnableLogStdout bool
	}
}
