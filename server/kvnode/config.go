package kvnode

import (
	"github.com/BurntSushi/toml"
	"sync/atomic"
	"unsafe"
)

var (
	defConfig *Config
)

func LoadConfigStr(str string) error {
	config := &Config{}
	_, err := toml.Decode(str, config)
	if nil != err {
		return err
	} else {
		atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&defConfig)), unsafe.Pointer(config))
		return nil
	}
}

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
	Shard []int

	SnapshotCurrentCount int //并行执行快照序列化数量，如果设置为0则取cpu数量

	LruCheckInterval int //ms

	CacheGroupSize       int
	MaxCachePerStoreSize int

	SqlLoadPipeLineSize int
	SqlLoadQueueSize    int

	SqlLoaderCount  int
	SqlUpdaterCount int

	ServiceHost string
	ServicePort int

	ProposalFlushInterval int
	ReadFlushInterval     int

	MainQueueMaxSize int //store主处理队列容量上限,超过上限客户端的命令无法入列将返回retry

	MaxPendingCmdCount      int // = int64(300000) //整个物理节点待处理的命令上限
	MaxPendingCmdCountPerKv int // = 100        //单个kv待处理命令上限

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
