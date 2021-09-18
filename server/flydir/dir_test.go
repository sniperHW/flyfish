package flydir

//go test -covermode=count -v -coverprofile=coverage.out -run=.
//go tool cover -html=coverage.out

import (
	"fmt"
	"github.com/sniperHW/flyfish/logger"
	fnet "github.com/sniperHW/flyfish/pkg/net"
	"github.com/sniperHW/flyfish/server/flygate"
	snet "github.com/sniperHW/flyfish/server/net"
	sproto "github.com/sniperHW/flyfish/server/proto"
	"github.com/stretchr/testify/assert"
	"net"
	"testing"
	"time"
)

var configGate1Str string = `
	ServiceHost = "localhost"
	ServicePort = 8110
	ConsolePort = 8110
	DirService  = "localhost:8113"

	MaxNodePendingMsg  = 4096
	MaxStorePendingMsg = 1024
	MaxPendingMsg = 10000

	[ClusterConfig]
		ClusterID               = 1
		DbHost                  = "localhost"
		DbPort                  = 5432
		DbUser			        = "sniper"
		DbPassword              = "123456"
		DbDataBase              = "test"

	[Log]
		MaxLogfileSize  = 104857600 # 100mb
		LogDir          = "log"
		LogPrefix       = "gate"
		LogLevel        = "info"
		EnableLogStdout = false		

`

var configGate2Str string = `
	ServiceHost = "localhost"
	ServicePort = 8111
	ConsolePort = 8111
	DirService  = "localhost:8113"

	MaxNodePendingMsg  = 4096
	MaxStorePendingMsg = 1024
	MaxPendingMsg = 10000

	[ClusterConfig]
		ClusterID               = 1
		DbHost                  = "localhost"
		DbPort                  = 5432
		DbUser			        = "sniper"
		DbPassword              = "123456"
		DbDataBase              = "test"

	[Log]
		MaxLogfileSize  = 104857600 # 100mb
		LogDir          = "log"
		LogPrefix       = "gate"
		LogLevel        = "info"
		EnableLogStdout = false		

`

var configDirStr string = `
	Host = "localhost"
	ConsolePort = 8113

	[ClusterConfig]
		ClusterID               = 1
		DbHost                  = "localhost"
		DbPort                  = 5432
		DbUser			        = "sniper"
		DbPassword              = "123456"
		DbDataBase              = "test"

	[Log]
		MaxLogfileSize  = 104857600 # 100mb
		LogDir          = "log"
		LogPrefix       = "gate"
		LogLevel        = "info"
		EnableLogStdout = false		

`

func Test1(t *testing.T) {

	l := logger.NewZapLogger("testDir.log", "./log", "Debug", 100, 14, true)
	InitLogger(l)
	flygate.InitLogger(l)

	configDir, err := LoadConfigStr(configDirStr)
	assert.Nil(t, err)

	dir := NewDir(configDir)

	err = dir.Start()

	assert.Nil(t, err)

	configGate1, err := flygate.LoadConfigStr(configGate1Str)
	assert.Nil(t, err)

	configGate2, err := flygate.LoadConfigStr(configGate2Str)
	assert.Nil(t, err)

	gate1 := flygate.NewGate(configGate1)
	gate2 := flygate.NewGate(configGate2)

	err = gate1.Start()
	assert.Nil(t, err)

	err = gate2.Start()
	assert.Nil(t, err)

	for {
		dir.mu.Lock()
		if len(dir.gates) == 2 {
			dir.mu.Unlock()
			break
		}
		dir.mu.Unlock()
		time.Sleep(time.Second)
	}

	{
		conn, err := fnet.NewUdp("localhost:0", snet.Pack, snet.Unpack)
		assert.Nil(t, err)

		addr, _ := net.ResolveUDPAddr("udp", "localhost:8113")
		conn.SendTo(addr, &sproto.QueryGateList{})
		recvbuff := make([]byte, 256)
		_, r, err := conn.ReadFrom(recvbuff)
		assert.Nil(t, err)
		assert.Equal(t, 2, len(r.(*sproto.GateList).List))
		fmt.Println(r.(*sproto.GateList).List)
		conn.Close()
	}

	gate1.Stop()

	for {
		dir.mu.Lock()
		if len(dir.gates) == 1 {
			dir.mu.Unlock()
			break
		}
		dir.mu.Unlock()
		time.Sleep(time.Second)
	}

	{
		conn, err := fnet.NewUdp("localhost:0", snet.Pack, snet.Unpack)
		assert.Nil(t, err)
		addr, _ := net.ResolveUDPAddr("udp", "localhost:8113")
		conn.SendTo(addr, &sproto.QueryGateList{})
		recvbuff := make([]byte, 256)
		_, r, err := conn.ReadFrom(recvbuff)
		assert.Nil(t, err)
		assert.Equal(t, 1, len(r.(*sproto.GateList).List))
		fmt.Println(r.(*sproto.GateList).List)
		conn.Close()
	}

	gate2.Stop()
	dir.Stop()

}
