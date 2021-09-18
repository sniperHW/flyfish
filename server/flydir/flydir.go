package flydir

import (
	"fmt"
	flynet "github.com/sniperHW/flyfish/pkg/net"
	"github.com/sniperHW/flyfish/server/clusterconf"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

type gate struct {
	service       string
	console       string
	timer         *time.Timer
	reportVersion int64
}

type dir struct {
	mu            sync.Mutex
	config        *Config
	consoleConn   *flynet.Udp
	updateing     bool
	kvconfVersion int
	stopOnce      int32
	startOnce     int32
	gates         map[string]*gate
}

func NewDir(config *Config) *dir {
	return &dir{
		config: config,
		gates:  map[string]*gate{},
	}
}

func (d *dir) Start() error {
	var err error
	if atomic.CompareAndSwapInt32(&d.startOnce, 0, 1) {
		config := d.config
		if err = os.MkdirAll(config.Log.LogDir, os.ModePerm); nil != err {
			return err
		}

		//从DB获取配置版本号
		clusterConf := config.ClusterConfig
		_, d.kvconfVersion, err = clusterconf.LoadConfigJsonFromDB(clusterConf.ClusterID, clusterConf.DBType, clusterConf.DBHost, clusterConf.DBPort, clusterConf.ConfDB, clusterConf.DBUser, clusterConf.DBPassword)
		if nil != err {
			return err
		}

		if err = d.initConsole(fmt.Sprintf("%s:%d", d.config.Host, d.config.ConsolePort)); nil != err {
			return err
		}
	}

	return nil
}

func (d *dir) Stop() {
	if atomic.CompareAndSwapInt32(&d.stopOnce, 0, 1) {
		d.consoleConn.Close()
	}
}
