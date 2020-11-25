package sqlnode

import (
	"github.com/BurntSushi/toml"
	"log"
	"sync/atomic"
	"unsafe"
)

type config struct {
	ServiceHost string

	ServicePort int

	DBConnections int

	Compress bool

	MaxRequestCount int

	DBConfig struct {
		SqlType string

		DbHost     string
		DbPort     int
		DbUser     string
		DbPassword string
		DbDataBase string

		ConfDbHost     string
		ConfDbPort     int
		ConfDbUser     string
		ConfDbPassword string
		ConfDataBase   string
	}

	Log struct {
		MaxLogfileSize  int
		LogDir          string
		LogPrefix       string
		LogLevel        string
		EnableLogStdout bool
	}
}

var (
	globalConf *config
)

func initConfig(filePath string) {
	conf := new(config)
	if _, err := toml.DecodeFile(filePath, conf); err != nil {
		log.Fatalf("init config: %s.\n", err)
	} else {
		atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&globalConf)), unsafe.Pointer(conf))
		log.Println("init config.")
	}
}

func getConfig() *config {
	return (*config)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&globalConf))))
}
