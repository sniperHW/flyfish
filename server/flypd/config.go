package flypd

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
	MainQueueMaxSize int //主处理队列容量上限,超过上限客户端的命令无法入列
	Log              struct {
		MaxLogfileSize  int
		LogDir          string
		LogPrefix       string
		LogLevel        string
		EnableLogStdout bool
	}
}
