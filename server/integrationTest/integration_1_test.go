//go test -coverpkg=github.com/sniperHW/flyfish/server/flygate,github.com/sniperHW/flyfish/server/flypd,github.com/sniperHW/flyfish/server/flykv -covermode=count -v -coverprofile=coverage.out -run=.
//go tool cover -html=coverage.out

package integrationTest

import (
	"fmt"
	"github.com/BurntSushi/toml"
	"github.com/sniperHW/flyfish/client"
	"github.com/sniperHW/flyfish/db"
	"github.com/sniperHW/flyfish/logger"
	fnet "github.com/sniperHW/flyfish/pkg/net"
	flygate "github.com/sniperHW/flyfish/server/flygate"
	flykv "github.com/sniperHW/flyfish/server/flykv"
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

func setMeta(t *testing.T) bool {
	m := &db.DbDef{
		TableDefs: []*db.TableDef{
			&db.TableDef{
				Name: "users1",
				Fields: []*db.FieldDef{
					&db.FieldDef{
						Name:        "name",
						Type:        "string",
						DefautValue: "",
					},
					&db.FieldDef{
						Name:        "age",
						Type:        "int",
						DefautValue: "",
					},
					&db.FieldDef{
						Name:        "phone",
						Type:        "string",
						DefautValue: "",
					},
				},
			},
		},
	}

	b, _ := db.DbDefToJsonString(m)

	conn, _ := fnet.NewUdp("localhost:0", snet.Pack, snet.Unpack)

	addr, _ := net.ResolveUDPAddr("udp", "localhost:8110")

	conn.SendTo(addr, snet.MakeMessage(0, &sproto.SetMeta{
		Meta: b,
	}))

	ch := make(chan interface{})

	go func() {
		recvbuff := make([]byte, 256)
		_, r, err := conn.ReadFrom(recvbuff)
		if nil == err {
			select {
			case ch <- r.(*snet.Message).Msg:
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

	if r == nil {
		return false
	} else {
		return r.(*sproto.SetMetaResp).Ok
	}

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

	conn.SendTo(addr, snet.MakeMessage(0, install))

	ch := make(chan interface{})

	go func() {
		recvbuff := make([]byte, 256)
		_, r, err := conn.ReadFrom(recvbuff)
		if nil == err {
			select {
			case ch <- r.(*snet.Message).Msg:
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

LinearizableRead        = true


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

var pdConfigStr string = `
	MainQueueMaxSize = 1000
	RaftLogDir              = "testRaftLog"
	RaftLogPrefix           = "flypd"
`

var flyGateConfigStr string = `  
	PdService="localhost:8110"
	MaxNodePendingMsg=2000
	MaxStorePendingMsg=2000
	MaxPendingMsg=20000	
`

func init() {
	l := logger.NewZapLogger("integrationTest.log", "./log", "Debug", 104857600, 14, 10, true)
	flypd.InitLogger(l)
	flykv.InitLogger(l)
	flygate.InitLogger(l)
	client.InitLogger(l)
	logger.InitLogger(l)
}

type StopAble interface {
	Stop()
}

func newPD(t *testing.T) StopAble {
	conf, _ := flypd.LoadConfigStr(pdConfigStr)

	pd, _ := flypd.NewPd(1, conf, "localhost:8110", "1@http://localhost:8110@voter", nil)

	for {
		if !setMeta(t) {
			time.Sleep(time.Second)
		} else {
			break
		}
	}

	for {
		if !installDeployment(t) {
			time.Sleep(time.Second)
		} else {
			break
		}
	}

	return pd
}

func TestFlygate(t *testing.T) {
	sslot.SlotCount = 128
	flypd.MinReplicaPerSet = 1
	flypd.StorePerSet = 1

	os.RemoveAll("./testRaftLog")

	pd := newPD(t)

	//启动kvnode
	var err error

	dbConf := &dbconf{}
	if _, err = toml.DecodeFile("test_dbconf.toml", dbConf); nil != err {
		panic(err)
	}

	kvConf, err := flykv.LoadConfigStr(fmt.Sprintf(flyKvConfigStr, dbConf.DBType, dbConf.Host, dbConf.Port, dbConf.Usr, dbConf.Pwd, dbConf.Db))

	if nil != err {
		panic(err)
	}

	node1, err := flykv.NewKvNode(1, kvConf, flykv.NewSqlDB())

	if nil != err {
		panic(err)
	}

	node2, err := flykv.NewKvNode(2, kvConf, flykv.NewSqlDB())

	if nil != err {
		panic(err)
	}

	//启动flygate

	gateConf, _ := flygate.LoadConfigStr(flyGateConfigStr)

	gate1, err := flygate.NewFlyGate(gateConf, "localhost:10110")

	if nil != err {
		panic(err)
	}

	for {

		addr, _ := net.ResolveUDPAddr("udp", "localhost:8110")

		gate := client.QueryGate([]*net.UDPAddr{addr})
		if len(gate) > 0 {
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

func TestAddRemoveNode(t *testing.T) {
	sslot.SlotCount = 128
	flypd.MinReplicaPerSet = 1
	flypd.StorePerSet = 1

	os.RemoveAll("./log")
	os.RemoveAll("./testRaftLog")

	var err error

	dbConf := &dbconf{}
	if _, err = toml.DecodeFile("test_dbconf.toml", dbConf); nil != err {
		panic(err)
	}

	kvConf, err := flykv.LoadConfigStr(fmt.Sprintf(flyKvConfigStr, dbConf.DBType, dbConf.Host, dbConf.Port, dbConf.Usr, dbConf.Pwd, dbConf.Db))

	if nil != err {
		panic(err)
	}

	pd := newPD(t)

	logger.GetSugar().Infof("testAddRemNode")

	conn, err := fnet.NewUdp("localhost:0", snet.Pack, snet.Unpack)
	assert.Nil(t, err)

	addr, _ := net.ResolveUDPAddr("udp", "localhost:8110")

	conn.SendTo(addr, snet.MakeMessage(0, &sproto.AddNode{
		SetID:       1,
		NodeID:      3,
		Host:        "localhost",
		ServicePort: 9320,
		RaftPort:    9321,
	}))

	recvbuff := make([]byte, 256)

	_, r, err := conn.ReadFrom(recvbuff)

	assert.Equal(t, true, r.(*snet.Message).Msg.(*sproto.AddNodeResp).Ok)

	node1, err := flykv.NewKvNode(1, kvConf, flykv.NewSqlDB())

	if nil != err {
		panic(err)
	}

	//add learnstore

	for {

		conn.SendTo(addr, snet.MakeMessage(0, &sproto.AddLearnerStoreToNode{
			SetID:  1,
			NodeID: 3,
			Store:  1,
		}))

		_, r, err = conn.ReadFrom(recvbuff)

		ret := r.(*snet.Message).Msg.(*sproto.AddLearnerStoreToNodeResp)

		if ret.Reason == "learner store already exists" {
			break
		}

		time.Sleep(time.Second)

	}

	logger.GetSugar().Infof("--------------------add learner 3:1 OK------------------------")

	node3, err := flykv.NewKvNode(3, kvConf, flykv.NewSqlDB())

	if nil != err {
		panic(err)
	}

	//promote to voter
	for {
		conn.SendTo(addr, snet.MakeMessage(0, &sproto.PromoteLearnerStore{
			SetID:  1,
			NodeID: 3,
			Store:  1,
		}))

		_, r, err = conn.ReadFrom(recvbuff)

		ret := r.(*snet.Message).Msg.(*sproto.PromoteLearnerStoreResp)

		if ret.Reason == "store is already a voter" {
			break
		}

		time.Sleep(time.Second)
	}

	/*
		//remove store
		fmt.Println("remove store")

		for {
			conn.SendTo(addr, snet.MakeMessage(0, &sproto.RemoveNodeStore{
				SetID:  1,
				NodeID: 3,
				Store:  1,
			}))

			_, r, err = conn.ReadFrom(recvbuff)

			ret := r.(*snet.Message).Msg.(*sproto.RemoveNodeStoreResp)

			fmt.Println(ret.Reason)

			if ret.Reason == "store not exists" {
				break
			}

			time.Sleep(time.Second)
		}

		//remove node
		fmt.Println("remove node")

		for {
			conn.SendTo(addr, snet.MakeMessage(0, &sproto.RemNode{
				SetID:  1,
				NodeID: 3,
			}))

			_, r, err = conn.ReadFrom(recvbuff)

			ret := r.(*snet.Message).Msg.(*sproto.RemNodeResp)

			if ret.Reason == "node not found" {
				break
			}
			time.Sleep(time.Second)
		}*/

	node3.Stop()

	node1.Stop()

	pd.Stop()

	conn.Close()
}
