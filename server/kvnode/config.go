package kvnode

import (
	"github.com/BurntSushi/toml"
)

func LoadConfigStr(str string) (*Config, error) {
	config := &Config{}
	_, err := toml.Decode(str, config)
	if nil != err {
		return nil, err
	} else {
		return config, nil
	}
}

func LoadConfig(path string) (*Config, error) {
	config := &Config{}
	_, err := toml.DecodeFile(path, config)
	if nil != err {
		return nil, err
	} else {
		return config, nil
	}
}

type Config struct {
	RaftUrl string

	Mode string //"cluster"集群模式,"solo"独立模式

	SoloConfig struct {
		ServiceHost string
		ServicePort int
		RaftCluster string
		Stores      []int
	}

	ClusterConfig struct {
		Pd []string //pd服务的地址
	}

	SnapshotCurrentCount int //并行执行快照序列化数量，如果设置为0则取cpu数量

	LruCheckInterval int //ms

	MaxCachePerStore int

	SqlLoaderCount int

	SqlUpdaterCount int

	ProposalFlushInterval int

	ReadFlushInterval int

	ProposalBatchCount int

	ReadBatchCount int

	MainQueueMaxSize int //store主处理队列容量上限,超过上限客户端的命令无法入列将返回retry

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
