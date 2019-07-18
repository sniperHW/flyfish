package conf

import "github.com/BurntSushi/toml"

const (
	MaxPacketSize = 4 * 1024 * 1024 // 4mb
)

var (
	DefConfig *Config
)

func InitConfig(path string) error {
	DefConfig = &Config{}
	_, err := toml.DecodeFile(path, DefConfig)
	return err
}

type Config struct {
	CacheGroupSize          int
	MaxCachePerGroupSize    int
	RedisProcessPoolSize    int
	SqlLoadPoolSize         int
	SqlUpdatePoolSize       int
	RedisPipelineSize       int
	SqlLoadPipeLineSize     int
	SqlUpdateEventQueueSize int
	SqlLoadEventQueueSize   int
	RedisEventQueueSize     int
	WriteBackEventQueueSize int
	WriteBackDelay          int64
	StrInitCap              int
	ServiceHost             string
	ServicePort             int
	BackDir                 string
	BackFile                string

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
