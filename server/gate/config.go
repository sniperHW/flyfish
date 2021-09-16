package gate

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
	ServiceHost string
	ServicePort int
	ConsolePort int

	ClusterConfig struct {
		ClusterID  int
		SqlType    string
		DbHost     string
		DbPort     int
		DbUser     string
		DbPassword string
		DbDataBase string
	}

	MaxNodePendingMsg  int
	MaxStorePendingMsg int

	Log struct {
		MaxLogfileSize  int
		LogDir          string
		LogPrefix       string
		LogLevel        string
		EnableLogStdout bool
	}
}
