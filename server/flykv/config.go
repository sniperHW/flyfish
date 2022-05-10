package flykv

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
	Mode                    string `toml:"Mode"` //"cluster"集群模式,"solo"独立模式
	SnapshotCount           uint64 `toml:"SnapshotCount"`
	SnapshotCatchUpEntriesN uint64 `toml:"SnapshotCatchUpEntriesN"`
	SnapshotBytes           uint64 `toml:"SnapshotBytes"`
	MaxBatchCount           int    `toml:"MaxBatchCount"`

	RaftLogDir string `toml:"RaftLogDir"` //raft日志存放目录

	RaftLogPrefix string `toml:"RaftLogPrefix"`

	SnapshotCurrentCount int `toml:"SnapshotCurrentCount"` //并行执行快照序列化数量，如果设置为0则取cpu数量

	MaxCachePerStore int `toml:"MaxCachePerStore"`

	SqlLoaderCount int `toml:"SqlLoaderCount"`

	SqlUpdaterCount int `toml:"SqlUpdaterCount"`

	MainQueueMaxSize int `toml:"MainQueueMaxSize"` //store主处理队列容量上限,超过上限客户端的命令无法入列将返回retry

	LinearizableRead bool `toml:"LinearizableRead"`

	WriteBackMode string `toml:"WriteBackMode"`

	SoloConfig struct {
		ServiceHost string `toml:"ServiceHost"`
		ServicePort int    `toml:"ServicePort"`
		RaftUrl     string `toml:"RaftUrl"`
		Stores      []int  `toml:"Stores"`
		MetaPath    string `toml:"MetaPath"`
	} `toml:"SoloConfig"`

	ClusterConfig struct {
		PD string `toml:"PD"` //pd服务地址用;分隔
	} `toml:"ClusterConfig"`

	DBConfig struct {
		DBType   string `toml:"DBType"`
		Host     string `toml:"Host"`
		Port     int    `toml:"Port"`
		User     string `toml:"User"`
		Password string `toml:"Password"`
		DB       string `toml:"DB"`
	} `toml:"DBConfig"`

	ReqLimit struct {
		SoftLimit        int `toml:"SoftLimit"`
		HardLimit        int `toml:"HardLimit"`
		SoftLimitSeconds int `toml:"SoftLimitSeconds"`
	} `toml:"ReqLimit"`

	Log struct {
		MaxLogfileSize int    `toml:"MaxLogfileSize"`
		LogDir         string `toml:"LogDir"`
		LogPrefix      string `toml:"LogPrefix"`
		LogLevel       string `toml:"LogLevel"`
		EnableStdout   bool   `toml:"EnableStdout"`
		MaxAge         int    `toml:"MaxAge"`
		MaxBackups     int    `toml:"MaxBackups"`
	} `toml:"Log"`
}
