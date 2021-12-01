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
	RaftLogDir    string //raft日志存放目录
	RaftLogPrefix string
	Log           struct {
		MaxLogfileSize int
		LogDir         string
		LogPrefix      string
		LogLevel       string
		EnableStdout   bool
		MaxAge         int
		MaxBackups     int
	}
}
