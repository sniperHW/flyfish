package flydir

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
	Host        string
	ConsolePort int

	ClusterConfig struct {
		ClusterID  int
		DBType     string
		DBHost     string
		DBPort     int
		DBUser     string
		DBPassword string
		ConfDB     string
	}

	Log struct {
		MaxLogfileSize  int
		LogDir          string
		LogPrefix       string
		LogLevel        string
		EnableLogStdout bool
	}
}
