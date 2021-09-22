package flypd

import (
	"github.com/BurntSushi/toml"
	"sync/atomic"
	"unsafe"
)

var (
	defConfig *Config
)

func LoadConfigStr(str string) error {
	config := &Config{}
	_, err := toml.Decode(str, config)
	if nil != err {
		return err
	} else {
		atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&defConfig)), unsafe.Pointer(config))
		return nil
	}
}

func LoadConfig(path string) error {
	config := &Config{}
	_, err := toml.DecodeFile(path, config)
	if nil != err {
		return err
	} else {
		atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&defConfig)), unsafe.Pointer(config))
		return nil
	}
}

func GetConfig() *Config {
	return (*Config)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&defConfig))))
}

type node struct {
	Id          int
	Service     string //对外服务地址端口
	RaftService string //raft服务地址
	UdpService  string
	Stores      []int
}

type kvnodes struct {
	Node []node
}

type Config struct {
	MainQueueMaxSize int //主处理队列容量上限,超过上限客户端的命令无法入列

	Stores []int

	Kvnodes kvnodes

	Log struct {
		MaxLogfileSize  int
		LogDir          string
		LogPrefix       string
		LogLevel        string
		EnableLogStdout bool
	}
}
