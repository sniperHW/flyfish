package config

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
	ClientType string `toml:"ClientType"` //FlyKv or FlySql
	Mode       string `toml:"Mode"`       //"cluster"集群模式,"solo"独立模式
	Service    string `toml:"Service"`
	PD         string `toml:"PD"` //pd服务地址用;分隔
	Stores     []int  `toml:"Stores"`
}
