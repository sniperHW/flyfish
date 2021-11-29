//go test -covermode=count -v -coverprofile=coverage.out -run=.
//go tool cover -html=coverage.out

package integrationTest

import (
	"fmt"
	"github.com/BurntSushi/toml"
	"github.com/sniperHW/flyfish/client"
	"github.com/sniperHW/flyfish/db/sql"
	"github.com/sniperHW/flyfish/logger"
	fnet "github.com/sniperHW/flyfish/pkg/net"
	flygate "github.com/sniperHW/flyfish/server/flygate"
	flykv "github.com/sniperHW/flyfish/server/flykv"
	"github.com/sniperHW/flyfish/server/flykv/metaLoader"
	flypd "github.com/sniperHW/flyfish/server/flypd"
	snet "github.com/sniperHW/flyfish/server/net"
	sproto "github.com/sniperHW/flyfish/server/proto"
	sslot "github.com/sniperHW/flyfish/server/slot"
	"github.com/stretchr/testify/assert"
	"net"
	"os"
	"testing"
	"time"
)

type dbconf struct {
	DBType string
	Host   string
	Port   int
	Usr    string
	Pwd    string
	Db     string
}

func installDeployment(t *testing.T) bool {

	conn, err := fnet.NewUdp("localhost:0", snet.Pack, snet.Unpack)
	assert.Nil(t, err)

	addr, _ := net.ResolveUDPAddr("udp", "localhost:8110")

	install := &sproto.InstallDeployment{}
	set1 := &sproto.DeploymentSet{SetID: 1}
	set1.Nodes = append(set1.Nodes, &sproto.DeploymentKvnode{
		NodeID:      1,
		Host:        "localhost",
		ServicePort: 9110,
		RaftPort:    9111,
	})
	install.Sets = append(install.Sets, set1)

	set2 := &sproto.DeploymentSet{SetID: 2}
	set2.Nodes = append(set2.Nodes, &sproto.DeploymentKvnode{
		NodeID:      2,
		Host:        "localhost",
		ServicePort: 9210,
		RaftPort:    9211,
	})
	install.Sets = append(install.Sets, set2)

	conn.SendTo(addr, install)

	ch := make(chan interface{})

	go func() {
		recvbuff := make([]byte, 256)
		_, r, err := conn.ReadFrom(recvbuff)
		if nil == err {
			select {
			case ch <- r:
			default:
			}
		}
	}()

	ticker := time.NewTicker(3 * time.Second)

	var r interface{}

	select {
	case r = <-ch:
	case <-ticker.C:
	}
	ticker.Stop()

	conn.Close()

	fmt.Println("installDeployment", r)

	if r == nil {
		return false
	} else {
		return r.(*sproto.InstallDeploymentResp).Ok
	}
}

var flyKvConfigStr string = `

Mode = "cluster"

DBType                  = "%s"

SnapshotCurrentCount    = 1

MainQueueMaxSize        = 10000

LruCheckInterval        = 1000              #每隔100ms执行一次lru剔除操作

MaxCachePerStore        = 100               #每组最大key数量，超过数量将会触发key剔除

SqlLoadPipeLineSize     = 200                  #sql加载管道线大小

SqlLoadQueueSize        = 10000                #sql加载请求队列大小，此队列每CacheGroup一个

SqlLoaderCount          = 5
SqlUpdaterCount         = 5

ProposalFlushInterval   = 100
ReadFlushInterval       = 10 

RaftLogDir              = "testRaftLog"

RaftLogPrefix           = "flykv"


[ClusterConfig]
PD                      = "localhost:8110"

[DBConfig]
Host          = "%s"
Port          = %d
User	      = "%s"
Password      = "%s"
DB            = "%s"

[StoreReqLimit]
SoftLimit               = 50000
HardLimit               = 100000
SoftLimitSeconds        = 10

`

func TestFlygate(t *testing.T) {
	sslot.SlotCount = 128
	flypd.KvNodePerSet = 1
	flypd.StorePerSet = 1

	os.RemoveAll("./testRaftLog")

	var configStr string = `

	MainQueueMaxSize = 1000
	RaftLogDir              = "testRaftLog"
	RaftLogPrefix           = "flypd"
`

	l := logger.NewZapLogger("integrationTest.log", "./log", "Debug", 104857600, 14, 10, true)
	flypd.InitLogger(l)
	flykv.InitLogger(l)
	flygate.InitLogger(l)
	client.InitLogger(l)

	conf, _ := flypd.LoadConfigStr(configStr)

	pd, _ := flypd.NewPd(conf, "localhost:8110", 1, "1@http://localhost:8110")

	for {
		if !installDeployment(t) {
			time.Sleep(time.Second)
		} else {
			break
		}
	}

	//启动kvnode
	var err error

	dbConf := &dbconf{}
	if _, err = toml.DecodeFile("test_dbconf.toml", dbConf); nil != err {
		panic(err)
	}

	meta, err := metaLoader.LoadDBMetaFromSqlCsv(dbConf.DBType, dbConf.Host, dbConf.Port, dbConf.Db, dbConf.Usr, dbConf.Pwd)

	if nil != err {
		panic(err)
	}

	kvConf, err := flykv.LoadConfigStr(fmt.Sprintf(flyKvConfigStr, dbConf.DBType, dbConf.Host, dbConf.Port, dbConf.Usr, dbConf.Pwd, dbConf.Db))

	if nil != err {
		panic(err)
	}

	node1 := flykv.NewKvNode(1, kvConf, meta, sql.CreateDbMeta, flykv.NewSqlDB())

	node2 := flykv.NewKvNode(2, kvConf, meta, sql.CreateDbMeta, flykv.NewSqlDB())

	if err = node1.Start(); nil != err {
		panic(err)
	}

	if err = node2.Start(); nil != err {
		panic(err)
	}

	//启动flygate
	var flyGateConfigStr string = `
	ServiceHost="localhost"
	ServicePort=10110     
	PdService="localhost:8110"
	MaxNodePendingMsg=2000
	MaxStorePendingMsg=2000
	MaxPendingMsg=20000	
	`

	gateConf, _ := flygate.LoadConfigStr(flyGateConfigStr)

	gate1 := flygate.NewFlyGate(gateConf)

	if err = gate1.Start(); nil != err {
		panic(err)
	}

	for {
		gate, err := client.QueryGate([]string{"localhost:8110"})
		if nil == err && gate != "" {
			break
		} else {
			time.Sleep(time.Second)
		}
	}

	fmt.Println("run client")

	c, _ := client.OpenClient(client.ClientConf{PD: []string{"localhost:8110"}})

	for i := 0; i < 20; i++ {
		fields := map[string]interface{}{}
		fields["age"] = 12
		name := fmt.Sprintf("sniperHW:%d", i)
		fields["name"] = name
		fields["phone"] = "123456789123456789123456789"

		r := c.Set("users1", name, fields).Exec()

		fmt.Println("set resp", r.ErrCode)
	}

	fmt.Println("Stop gate")

	gate1.Stop()

	fmt.Println("Stop node1")

	node1.Stop()

	fmt.Println("Stop node2")

	node2.Stop()

	fmt.Println("Stop pd")
	pd.Stop()

}
