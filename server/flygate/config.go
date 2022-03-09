package flygate

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
	PdService string `toml:"PdService"` //pd服务以；分割

	//MaxNodePendingMsg  int    `toml:"MaxNodePendingMsg"`  //单个node  dial期间pending消息数量限制/已经发往kvnode等待应答的消息数量
	//MaxStorePendingMsg int    `toml:"MaxStorePendingMsg"` //单个store 缺失leader期间pending消息数量限制
	//MaxPendingMsg      int    `toml:"MaxPendingMsg"`      //整个gate pending消息数量限制

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
