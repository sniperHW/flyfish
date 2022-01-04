//go test -coverpkg=github.com/sniperHW/flyfish/server/flygate,github.com/sniperHW/flyfish/server/flypd,github.com/sniperHW/flyfish/server/flykv -covermode=count -v -coverprofile=coverage.out -run=.
//go tool cover -html=coverage.out

package integrationTest

import (
	"fmt"
	"github.com/BurntSushi/toml"
	"github.com/sniperHW/flyfish/client"
	"github.com/sniperHW/flyfish/logger"
	"github.com/sniperHW/flyfish/pkg/bitmap"
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
	InitDepoymentPath       = "./deployment.json"
	InitMetaPath            = "./meta.json"	
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
	pd, _ := flypd.NewPd(1, false, conf, "localhost:8110", "1@http://localhost:8110@voter")
	return pd
}

func TestFlygate(t *testing.T) {
	sslot.SlotCount = 128
	flypd.MinReplicaPerSet = 1
	flypd.StorePerSet = 2

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

	node1, err := flykv.NewKvNode(1, false, kvConf, flykv.NewSqlDB())

	if nil != err {
		panic(err)
	}

	node2, err := flykv.NewKvNode(2, false, kvConf, flykv.NewSqlDB())

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

	for i := 0; i < 100; i++ {
		fields := map[string]interface{}{}
		fields["age"] = 12
		name := fmt.Sprintf("sniperHW:%d", i)
		fields["name"] = name
		fields["phone"] = "123456789123456789123456789"

		beg := time.Now()
		for {
			r := c.Set("users1", name, fields).Exec()
			if nil == r.ErrCode {
				break
			} else {
				fmt.Println(name, r.ErrCode)
			}
		}

		fmt.Printf("use %v\n", time.Now().Sub(beg))

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

	for {
		resp := snet.UdpCall([]*net.UDPAddr{addr},
			snet.MakeMessage(0, &sproto.AddNode{
				SetID:       1,
				NodeID:      3,
				Host:        "localhost",
				ServicePort: 9320,
				RaftPort:    9321,
			}),
			time.Second,
			func(respCh chan interface{}, r interface{}) {
				if m, ok := r.(*snet.Message); ok {
					if resp, ok := m.Msg.(*sproto.AddNodeResp); ok {
						select {
						case respCh <- resp:
						default:
						}
					}
				}
			})

		if resp != nil && resp.(*sproto.AddNodeResp).Reason == "duplicate node id" {
			break
		}
	}

	node1, err := flykv.NewKvNode(1, false, kvConf, flykv.NewSqlDB())

	if nil != err {
		panic(err)
	}

	logger.GetSugar().Infof("add node")

	//add learnstore

	for {
		resp := snet.UdpCall([]*net.UDPAddr{addr},
			snet.MakeMessage(0, &sproto.AddLearnerStoreToNode{
				SetID:  1,
				NodeID: 3,
				Store:  1,
			}),
			time.Second,
			func(respCh chan interface{}, r interface{}) {
				if m, ok := r.(*snet.Message); ok {
					if resp, ok := m.Msg.(*sproto.AddLearnerStoreToNodeResp); ok {
						select {
						case respCh <- resp:
						default:
						}
					}
				}
			})

		if resp != nil && resp.(*sproto.AddLearnerStoreToNodeResp).Reason == "learner store already exists" {
			break
		}
	}

	logger.GetSugar().Infof("--------------------add learner 3:1 OK------------------------")

	node3, err := flykv.NewKvNode(3, true, kvConf, flykv.NewSqlDB())

	if nil != err {
		panic(err)
	}

	//promote to voter
	for {
		resp := snet.UdpCall([]*net.UDPAddr{addr},
			snet.MakeMessage(0, &sproto.PromoteLearnerStore{
				SetID:  1,
				NodeID: 3,
				Store:  1,
			}),
			time.Second,
			func(respCh chan interface{}, r interface{}) {
				if m, ok := r.(*snet.Message); ok {
					if resp, ok := m.Msg.(*sproto.PromoteLearnerStoreResp); ok {
						select {
						case respCh <- resp:
						default:
						}
					}
				}
			})

		if resp != nil && resp.(*sproto.PromoteLearnerStoreResp).Reason == "store is already a voter" {
			break
		}

	}

	//remove store
	fmt.Println("remove store")

	for {
		resp := snet.UdpCall([]*net.UDPAddr{addr},
			snet.MakeMessage(0, &sproto.RemoveNodeStore{
				SetID:  1,
				NodeID: 3,
				Store:  1,
			}),
			time.Second,
			func(respCh chan interface{}, r interface{}) {
				if m, ok := r.(*snet.Message); ok {
					if resp, ok := m.Msg.(*sproto.RemoveNodeStoreResp); ok {
						select {
						case respCh <- resp:
						default:
						}
					}
				}
			})

		if resp != nil && resp.(*sproto.RemoveNodeStoreResp).Reason == "store not exists" {
			break
		}
	}

	//remove node
	fmt.Println("remove node")

	for {
		resp := snet.UdpCall([]*net.UDPAddr{addr},
			snet.MakeMessage(0, &sproto.RemNode{
				SetID:  1,
				NodeID: 3,
			}),
			time.Second,
			func(respCh chan interface{}, r interface{}) {
				if m, ok := r.(*snet.Message); ok {
					if resp, ok := m.Msg.(*sproto.RemNodeResp); ok {
						select {
						case respCh <- resp:
						default:
						}
					}
				}
			})

		if resp != nil && resp.(*sproto.RemNodeResp).Reason == "node not found" {
			break
		}
	}

	node3.Stop()

	node1.Stop()

	pd.Stop()

	conn.Close()
}

func TestAddSet(t *testing.T) {
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

	addr, _ := net.ResolveUDPAddr("udp", "localhost:8110")

	for {
		resp := snet.UdpCall([]*net.UDPAddr{addr},
			snet.MakeMessage(0, &sproto.AddSet{
				&sproto.DeploymentSet{
					SetID: 3,
					Nodes: []*sproto.DeploymentKvnode{
						&sproto.DeploymentKvnode{
							NodeID:      3,
							Host:        "localhost",
							ServicePort: 9311,
							RaftPort:    9321,
						},
					},
				},
			}),
			time.Second,
			func(respCh chan interface{}, r interface{}) {
				if m, ok := r.(*snet.Message); ok {
					if resp, ok := m.Msg.(*sproto.AddSetResp); ok {
						select {
						case respCh <- resp:
						default:
						}
					}
				}
			})

		if resp != nil && resp.(*sproto.AddSetResp).Reason == "set already exists" {
			break
		}
	}

	node1, err := flykv.NewKvNode(1, false, kvConf, flykv.NewSqlDB())

	if nil != err {
		panic(err)
	}

	node2, err := flykv.NewKvNode(2, false, kvConf, flykv.NewSqlDB())

	if nil != err {
		panic(err)
	}

	node3, err := flykv.NewKvNode(3, false, kvConf, flykv.NewSqlDB())

	if nil != err {
		panic(err)
	}

	//等待
	for {
		resp := snet.UdpCall([]*net.UDPAddr{addr},
			snet.MakeMessage(0, &sproto.GetSetStatus{}),
			time.Second,
			func(respCh chan interface{}, r interface{}) {
				if m, ok := r.(*snet.Message); ok {
					if resp, ok := m.Msg.(*sproto.GetSetStatusResp); ok {
						select {
						case respCh <- resp:
						default:
						}
					}
				}
			})

		if resp != nil {
			slotPerStore := sslot.SlotCount / 3

			ok := true
			for _, v := range resp.(*sproto.GetSetStatusResp).Sets {
				for _, vv := range v.Stores {
					slots, _ := bitmap.CreateFromJson(vv.Slots)
					if len(slots.GetOpenBits()) > slotPerStore+1 {
						ok = false
					}
				}
			}

			if ok {
				slots1, _ := bitmap.CreateFromJson(resp.(*sproto.GetSetStatusResp).Sets[0].Stores[0].Slots)
				logger.GetSugar().Infof("%d %v", len(slots1.GetOpenBits()), slots1.GetOpenBits())
				slots2, _ := bitmap.CreateFromJson(resp.(*sproto.GetSetStatusResp).Sets[1].Stores[0].Slots)
				logger.GetSugar().Infof("%d %v", len(slots2.GetOpenBits()), slots2.GetOpenBits())
				slots3, _ := bitmap.CreateFromJson(resp.(*sproto.GetSetStatusResp).Sets[2].Stores[0].Slots)
				logger.GetSugar().Infof("%d %v", len(slots3.GetOpenBits()), slots3.GetOpenBits())
				break
			}
		}
	}

	for {
		resp := snet.UdpCall([]*net.UDPAddr{addr},
			snet.MakeMessage(0, &sproto.SetMarkClear{
				SetID: 3,
			}),
			time.Second,
			func(respCh chan interface{}, r interface{}) {
				if m, ok := r.(*snet.Message); ok {
					if resp, ok := m.Msg.(*sproto.SetMarkClearResp); ok {
						select {
						case respCh <- resp:
						default:
						}
					}
				}
			})

		if resp != nil && resp.(*sproto.SetMarkClearResp).Reason == "already mark clear" {
			break
		}
	}

	//等待
	for {
		resp := snet.UdpCall([]*net.UDPAddr{addr},
			snet.MakeMessage(0, &sproto.GetSetStatus{}),
			time.Second,
			func(respCh chan interface{}, r interface{}) {
				if m, ok := r.(*snet.Message); ok {
					if resp, ok := m.Msg.(*sproto.GetSetStatusResp); ok {
						select {
						case respCh <- resp:
						default:
						}
					}
				}
			})

		if resp != nil {
			var set1 *sproto.SetStatus
			var set2 *sproto.SetStatus
			var set3 *sproto.SetStatus
			for _, v := range resp.(*sproto.GetSetStatusResp).Sets {
				if v.SetID == 1 {
					set1 = v
				} else if v.SetID == 2 {
					set2 = v
				} else if v.SetID == 3 {
					set3 = v
				}
			}

			slots1, _ := bitmap.CreateFromJson(set1.Stores[0].Slots)
			slots2, _ := bitmap.CreateFromJson(set2.Stores[0].Slots)
			slots3, _ := bitmap.CreateFromJson(set3.Stores[0].Slots)

			if len(slots3.GetOpenBits()) == 0 && len(slots1.GetOpenBits())+len(slots2.GetOpenBits()) == sslot.SlotCount {
				logger.GetSugar().Infof("%d %v", len(slots1.GetOpenBits()), slots1.GetOpenBits())
				logger.GetSugar().Infof("%d %v", len(slots2.GetOpenBits()), slots2.GetOpenBits())
				break
			}
		}
	}

	for {
		resp := snet.UdpCall([]*net.UDPAddr{addr},
			snet.MakeMessage(0, &sproto.RemSet{
				SetID: 3,
			}),
			time.Second,
			func(respCh chan interface{}, r interface{}) {
				if m, ok := r.(*snet.Message); ok {
					if resp, ok := m.Msg.(*sproto.RemSetResp); ok {
						select {
						case respCh <- resp:
						default:
						}
					}
				}
			})

		if resp != nil && resp.(*sproto.RemSetResp).Reason == "set not exists" {
			break
		}
	}

	pd.Stop()
	node1.Stop()
	node2.Stop()
	node3.Stop()

}
