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
	RaftLogDir        string `toml:"RaftLogDir"` //raft日志存放目录
	RaftLogPrefix     string `toml:"RaftLogPrefix"`
	InitDepoymentPath string `toml:"InitDepoymentPath"`
	InitMetaPath      string `toml:"InitMetaPath"`
	DisableUdpConsole bool   `toml:"DisableUdpConsole"` //是否禁止udp控制台命令

	TLS struct {
		EnableTLS bool   `toml:"EnableTLS"`
		Key       string `toml:"Key"`
		Crt       string `toml:"Crt"`
	} `toml:"Tls"`

	DBType string `toml:"DBType"`

	DBConfig struct {
		Host     string `toml:"Host"`
		Port     int    `toml:"Port"`
		User     string `toml:"User"`
		Password string `toml:"Password"`
		DB       string `toml:"DB"`
	} `toml:"DBConfig"`

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
