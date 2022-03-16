package flybloom

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
	PD            string  `toml:"PD"`          //pd服务地址用;分隔
	MaxElements   uint64  `toml:"MaxElements"` //100000000
	ProbCollide   float64 `toml:"ProbCollide"` //0.00001
	RaftLogDir    string  `toml:"RaftLogDir"`  //raft日志存放目录
	RaftLogPrefix string  `toml:"RaftLogPrefix"`
	Log           struct {
		MaxLogfileSize int    `toml:"MaxLogfileSize"`
		LogDir         string `toml:"LogDir"`
		LogPrefix      string `toml:"LogPrefix"`
		LogLevel       string `toml:"LogLevel"`
		EnableStdout   bool   `toml:"EnableStdout"`
		MaxAge         int    `toml:"MaxAge"`
		MaxBackups     int    `toml:"MaxBackups"`
	} `toml:"Log"`
}
