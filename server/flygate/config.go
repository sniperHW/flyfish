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
	//ServiceHost        string
	//ServicePort        int

	PdService          string //pd服务以；分割
	MaxNodePendingMsg  int    //单个node  dial期间pending消息数量限制/已经发往kvnode等待应答的消息数量
	MaxStorePendingMsg int    //单个store 缺失leader期间pending消息数量限制
	MaxPendingMsg      int    //整个gate pending消息数量限制

	Log struct {
		MaxLogfileSize int
		LogDir         string
		LogPrefix      string
		LogLevel       string
		EnableStdout   bool
		MaxAge         int
		MaxBackups     int
	}
}
