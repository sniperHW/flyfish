package conf

import "github.com/BurntSushi/toml"

type Config struct {
	HttpAddr string
	LoadDir  string
}

var config *Config

func LoadConfig(path string) {
	config = &Config{}
	_, err := toml.DecodeFile(path, config)
	if err != nil {
		panic(err)
	}
}

func GetConfig() *Config {
	return config
}
