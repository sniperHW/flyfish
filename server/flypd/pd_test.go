package flypd

//go test -race -covermode=atomic -v -coverprofile=coverage.out -run=.
//go tool cover -html=coverage.out

import (
	"fmt"
	"github.com/sniperHW/flyfish/db"
	"github.com/sniperHW/flyfish/db/sql"
	"github.com/sniperHW/flyfish/logger"
	"github.com/sniperHW/flyfish/pkg/etcd/pkg/idutil"
	fnet "github.com/sniperHW/flyfish/pkg/net"
	console "github.com/sniperHW/flyfish/server/flypd/console/http"
	consoleUdp "github.com/sniperHW/flyfish/server/flypd/console/udp"
	snet "github.com/sniperHW/flyfish/server/net"
	sproto "github.com/sniperHW/flyfish/server/proto"
	sslot "github.com/sniperHW/flyfish/server/slot"
	"github.com/stretchr/testify/assert"
	"net"
	"os"
	"sync"
	"testing"
	"time"
)

var RaftIDGen *idutil.Generator = idutil.NewGenerator(1, time.Now())

func init() {
	sslot.SlotCount = 128
}

func waitCondition(fn func() bool) {
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		for {
			time.Sleep(time.Millisecond * 100)
			if fn() {
				wg.Done()
				break
			}
		}
	}()
	wg.Wait()
}

var configStr string = `

	MainQueueMaxSize = 1000
	RaftLogDir       = "raftLog"
	RaftLogPrefix    = "pd"
	DBType           = "pgsql"
	InitMetaPath = "./initmeta.json"
	InitDepoymentPath="./deployment.json"


	[DBConfig]
		Host          = "localhost"
		Port          = 5432
		User	      = "sniper"
		Password      = "123456"
		DB            = "test"

	[Log]
		MaxLogfileSize  = 104857600 # 100mb
		LogDir          = "log"
		LogPrefix       = "pd"
		LogLevel        = "info"
		EnableLogStdout = false	
		MaxAge          = 14
		MaxBackups      = 10			

`

func TestPd1(t *testing.T) {

	os.RemoveAll("./raftLog")

	l := logger.NewZapLogger("testPd.log", "./log", "Debug", 100, 14, 10, true)
	InitLogger(l)

	conf, _ := LoadConfigStr(configStr)

	fmt.Println("conf.DBType", conf.DBType)

	//先删除table1
	dbc, err := sql.SqlOpen(conf.DBType, conf.DBConfig.Host, conf.DBConfig.Port, conf.DBConfig.DB, conf.DBConfig.User, conf.DBConfig.Password)

	fmt.Println(err)

	sql.DropTable(dbc, conf.DBType, &db.TableDef{
		Name:      "table1",
		DbVersion: 0,
	})

	sql.DropTable(dbc, conf.DBType, &db.TableDef{
		Name:      "table2",
		DbVersion: 1,
	})

	sql.DropTable(dbc, conf.DBType, &db.TableDef{
		Name:      "table2",
		DbVersion: 3,
	})

	raftID := RaftIDGen.Next()

	p, _ := NewPd(1, 1, false, conf, fmt.Sprintf("1@%d@http://localhost:18110@localhost:8110@", raftID))

	for {
		if p.isLeader() {
			break
		} else {
			time.Sleep(time.Second)
		}
	}

	testAddRemoveTable(t, p)

	testAddRemoveFields(t, p)

	testAddRemNode(t, p)

	testAddRemSet(t, p)

	testAddRemNode(t, p)

	testSlotTransfer(t, p)

	p.Stop()

	p, _ = NewPd(1, 1, false, conf, fmt.Sprintf("1@%d@http://localhost:18110@localhost:8110@", raftID))

	for {
		if p.isLeader() {
			break
		} else {
			time.Sleep(time.Second)
		}
	}

	p.Stop()

}

func TestAddRemovePd(t *testing.T) {
	os.RemoveAll("./raftLog")

	l := logger.NewZapLogger("testPd.log", "./log", "Debug", 100, 14, 10, true)
	InitLogger(l)

	conf, _ := LoadConfigStr(configStr)

	fmt.Println("conf.DBType", conf.DBType)

	raftID := RaftIDGen.Next()

	p1, _ := NewPd(1, 1, false, conf, fmt.Sprintf("1@%d@http://localhost:18110@localhost:8110@", raftID))

	for {
		if p1.isLeader() {
			break
		} else {
			time.Sleep(time.Second)
		}
	}

	conn, err := fnet.NewUdp("localhost:0", snet.Pack, snet.Unpack)
	assert.Nil(t, err)

	addr, _ := net.ResolveUDPAddr("udp", "localhost:8110")

	var raftCluster string
	var cluster int

	{
		conn.SendTo(addr, snet.MakeMessage(0, &sproto.AddPdNode{
			Id:        2,
			ClientUrl: "localhost:8111",
			Url:       "http://localhost:18111",
		}))

		recvbuff := make([]byte, 256)
		_, r, _ := conn.ReadFrom(recvbuff)

		ret := r.(*snet.Message).Msg.(*sproto.AddPdNodeResp)

		raftID = ret.RaftID
		raftCluster = ret.RaftCluster
		cluster = int(ret.Cluster)
	}

	p2, err := NewPd(2, cluster, true, conf, raftCluster)

	time.Sleep(time.Second)

	for {
		if resp, err := consoleUdp.Call([]string{"localhost:8110", "localhost:8111"}, &sproto.ListPdMembers{}, time.Second); nil != resp {
			GetSugar().Infof("%v", resp.(*sproto.ListPdMembersResp).Members)
			break
		} else {
			GetSugar().Errorf("ListPdMembers err:%v %v", err, resp)
		}
		time.Sleep(time.Second)
	}

	for {
		if resp, err := consoleUdp.Call([]string{"localhost:8110", "localhost:8111"}, &sproto.RemovePdNode{
			Id:     2,
			RaftID: raftID,
		}, time.Second); nil != resp && resp.(*sproto.RemovePdNodeResp).Reason == "membership: ID not found" {
			break
		} else {
			GetSugar().Errorf("RemovePdNode err:%v %v", err, resp)
		}
		time.Sleep(time.Second)
	}

	time.Sleep(time.Second)

	p1.Stop()

	p2.Stop()

}

func TestHttp(t *testing.T) {

	os.RemoveAll("./raftLog")

	l := logger.NewZapLogger("testPd.log", "./log", "Debug", 100, 14, 10, true)
	InitLogger(l)

	conf, _ := LoadConfigStr(configStr)

	raftID := RaftIDGen.Next()

	p, _ := NewPd(1, 1, false, conf, fmt.Sprintf("1@%d@http://localhost:18110@localhost:8110@", raftID))

	addr, _ := net.ResolveUDPAddr("udp", "localhost:8110")

	for {
		if resp, err := consoleUdp.Call([]*net.UDPAddr{addr}, &sproto.GetMeta{}, time.Second); nil != resp && resp.(*sproto.GetMetaResp).Version > 0 {
			break
		} else {
			fmt.Println(resp, err)
		}
		time.Sleep(time.Second)
	}

	testHttp(t)

	p.Stop()
}

func testHttp(t *testing.T) {
	c := console.NewClient("localhost:8110")
	resp, err := c.Call(&sproto.GetMeta{}, &sproto.GetMetaResp{})
	fmt.Println(resp, err)
}

func testAddRemoveTable(t *testing.T, p *pd) {

	GetSugar().Infof("testAddRemoveTable")

	//add table2
	{
		req := &sproto.MetaAddTable{
			Name:    "table2",
			Version: 1,
		}

		req.Fields = append(req.Fields, &sproto.MetaFiled{
			Name:    "field1",
			Type:    "string",
			Default: "hello",
		})

		conn, err := fnet.NewUdp("localhost:0", snet.Pack, snet.Unpack)
		assert.Nil(t, err)

		addr, _ := net.ResolveUDPAddr("udp", "localhost:8110")

		conn.SendTo(addr, snet.MakeMessage(0, req))

		recvbuff := make([]byte, 256)
		_, r, err := conn.ReadFrom(recvbuff)

		assert.Equal(t, r.(*snet.Message).Msg.(*sproto.MetaAddTableResp).Ok, true)
		conn.Close()
	}

	//remove table2
	{
		req := &sproto.MetaRemoveTable{
			Table:   "table2",
			Version: 2,
		}

		conn, err := fnet.NewUdp("localhost:0", snet.Pack, snet.Unpack)
		assert.Nil(t, err)

		addr, _ := net.ResolveUDPAddr("udp", "localhost:8110")

		conn.SendTo(addr, snet.MakeMessage(0, req))

		recvbuff := make([]byte, 256)
		_, r, err := conn.ReadFrom(recvbuff)

		assert.Equal(t, r.(*snet.Message).Msg.(*sproto.MetaRemoveTableResp).Ok, true)
		conn.Close()
	}

	//add table2 again
	{
		req := &sproto.MetaAddTable{
			Name:    "table2",
			Version: 3,
		}

		req.Fields = append(req.Fields, &sproto.MetaFiled{
			Name:    "field1",
			Type:    "string",
			Default: "hello",
		})

		conn, err := fnet.NewUdp("localhost:0", snet.Pack, snet.Unpack)
		assert.Nil(t, err)

		addr, _ := net.ResolveUDPAddr("udp", "localhost:8110")

		conn.SendTo(addr, snet.MakeMessage(0, req))

		recvbuff := make([]byte, 256)
		_, r, err := conn.ReadFrom(recvbuff)

		assert.Equal(t, r.(*snet.Message).Msg.(*sproto.MetaAddTableResp).Ok, true)
		conn.Close()
	}
}

func testAddRemoveFields(t *testing.T, p *pd) {

	GetSugar().Infof("testAddRemoveFields")

	conn, err := fnet.NewUdp("localhost:0", snet.Pack, snet.Unpack)
	assert.Nil(t, err)

	addr, _ := net.ResolveUDPAddr("udp", "localhost:8110")

	//add field
	{
		req := &sproto.MetaAddFields{
			Table:   "table1",
			Version: 4,
		}

		req.Fields = append(req.Fields, &sproto.MetaFiled{
			Name:    "field4",
			Type:    "string",
			Default: "hello",
		})

		conn.SendTo(addr, snet.MakeMessage(0, req))

		recvbuff := make([]byte, 256)
		_, r, _ := conn.ReadFrom(recvbuff)

		fmt.Println(r.(*snet.Message).Msg.(*sproto.MetaAddFieldsResp).Reason)

		assert.Equal(t, r.(*snet.Message).Msg.(*sproto.MetaAddFieldsResp).Ok, true)
	}

	//remove field
	{
		req := &sproto.MetaRemoveFields{
			Table:   "table1",
			Version: 5,
		}

		req.Fields = append(req.Fields, "field4")

		conn.SendTo(addr, snet.MakeMessage(0, req))

		recvbuff := make([]byte, 256)
		_, r, _ := conn.ReadFrom(recvbuff)

		assert.Equal(t, r.(*snet.Message).Msg.(*sproto.MetaRemoveFieldsResp).Ok, true)
	}

	//add field again
	{
		req := &sproto.MetaAddFields{
			Table:   "table1",
			Version: 6,
		}

		req.Fields = append(req.Fields, &sproto.MetaFiled{
			Name: "field4",
			Type: "int",
		})

		conn.SendTo(addr, snet.MakeMessage(0, req))

		recvbuff := make([]byte, 256)
		_, r, _ := conn.ReadFrom(recvbuff)

		assert.Equal(t, r.(*snet.Message).Msg.(*sproto.MetaAddFieldsResp).Ok, true)
	}

	conn.Close()

}

func testAddRemNode(t *testing.T, p *pd) {

	GetSugar().Infof("testAddRemNode")

	conn, err := fnet.NewUdp("localhost:0", snet.Pack, snet.Unpack)
	assert.Nil(t, err)

	addr, _ := net.ResolveUDPAddr("udp", "localhost:8110")

	conn.SendTo(addr, snet.MakeMessage(0, &sproto.AddNode{
		SetID:       1,
		NodeID:      2,
		Host:        "localhost",
		ServicePort: 9120,
		RaftPort:    9121,
	}))

	recvbuff := make([]byte, 256)

	_, r, err := conn.ReadFrom(recvbuff)

	assert.Equal(t, true, r.(*snet.Message).Msg.(*sproto.AddNodeResp).Ok)

	node1 := &testKvnode{
		nodeId: 1,
	}

	node1.udp, _ = fnet.NewUdp("localhost:9110", snet.Pack, snet.Unpack)

	go node1.run()

	//add learnstore
	conn.SendTo(addr, snet.MakeMessage(0, &sproto.AddLearnerStoreToNode{
		SetID:  1,
		NodeID: 2,
		Store:  1,
	}))

	_, r, err = conn.ReadFrom(recvbuff)

	assert.Equal(t, true, r.(*snet.Message).Msg.(*sproto.AddLearnerStoreToNodeResp).Ok)

	fmt.Println("promote to voter")

	//promote to voter
	for {
		conn.SendTo(addr, snet.MakeMessage(0, &sproto.PromoteLearnerStore{
			SetID:  1,
			NodeID: 2,
			Store:  1,
		}))

		_, r, err = conn.ReadFrom(recvbuff)

		ret := r.(*snet.Message).Msg.(*sproto.PromoteLearnerStoreResp)

		if ret.Reason == "store is already a voter" {
			break
		}

		time.Sleep(time.Second)
	}

	//remove store
	fmt.Println("remove store")

	for {
		conn.SendTo(addr, snet.MakeMessage(0, &sproto.RemoveNodeStore{
			SetID:  1,
			NodeID: 2,
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

	for {

		conn.SendTo(addr, snet.MakeMessage(0, &sproto.RemNode{
			SetID:  1,
			NodeID: 2,
		}))

		_, r, err = conn.ReadFrom(recvbuff)

		ret := r.(*snet.Message).Msg.(*sproto.RemNodeResp)

		if ret.Reason == "node not found" {
			break
		}
		time.Sleep(time.Second)

	}

	waitCondition(func() bool {
		ret := make(chan bool, 1)
		p.mainque.AppendHighestPriotiryItem(func() {
			if len(p.pState.deployment.sets[1].nodes) == 1 {
				ret <- true
			} else {
				ret <- false
			}
		})
		return <-ret
	})

	node1.stop()

	conn.Close()
}

func testAddRemSet(t *testing.T, p *pd) {

	GetSugar().Infof("testAddRemSet")

	conn, err := fnet.NewUdp("localhost:0", snet.Pack, snet.Unpack)
	assert.Nil(t, err)

	addr, _ := net.ResolveUDPAddr("udp", "localhost:8110")

	conn.SendTo(addr, snet.MakeMessage(0, &sproto.AddSet{
		&sproto.DeploymentSet{
			SetID: 2,
			Nodes: []*sproto.DeploymentKvnode{
				&sproto.DeploymentKvnode{
					NodeID:      3,
					Host:        "localhost",
					ServicePort: 8311,
					RaftPort:    8321,
				},
			},
		},
	}))

	recvbuff := make([]byte, 256)

	_, r, err := conn.ReadFrom(recvbuff)

	fmt.Println(r.(*snet.Message).Msg.(*sproto.AddSetResp).Reason)

	assert.Equal(t, true, r.(*snet.Message).Msg.(*sproto.AddSetResp).Ok)

	waitCondition(func() bool {
		ret := make(chan bool, 1)
		p.mainque.AppendHighestPriotiryItem(func() {
			if len(p.pState.deployment.sets) == 2 {
				ret <- true
			} else {
				ret <- false
			}
		})
		return <-ret
	})

	assert.Equal(t, 3, int(p.pState.deployment.sets[2].nodes[3].id))

	conn.SendTo(addr, snet.MakeMessage(0, &sproto.RemSet{
		SetID: 2,
	}))

	_, r, err = conn.ReadFrom(recvbuff)

	assert.Equal(t, false, r.(*snet.Message).Msg.(*sproto.RemSetResp).Ok)

	conn.SendTo(addr, snet.MakeMessage(0, &sproto.SetMarkClear{
		SetID: 2,
	}))

	_, r, err = conn.ReadFrom(recvbuff)

	assert.Equal(t, true, r.(*snet.Message).Msg.(*sproto.SetMarkClearResp).Ok)

	waitCondition(func() bool {
		ret := make(chan bool, 1)
		p.mainque.AppendHighestPriotiryItem(func() {
			if len(p.pState.deployment.sets) == 1 {
				ret <- true
			} else {
				conn.SendTo(addr, snet.MakeMessage(0, &sproto.RemSet{
					SetID: 2,
				}))
				ret <- false
			}
		})
		return <-ret
	})
}

type testKvnode struct {
	udp         *fnet.Udp
	nodeId      int
	metaVersion int64
}

func (n *testKvnode) run() {
	recvbuff := make([]byte, 64*1024)
	for {
		from, m, err := n.udp.ReadFrom(recvbuff)
		if nil != err {
			fmt.Println("ReadFrom", err)
			return
		} else {
			msg := m.(*snet.Message).Msg
			context := m.(*snet.Message).Context
			switch msg.(type) {
			case *sproto.NotifyNodeStoreOp:
				fmt.Println("on NotifyNodeStoreOp")
				n.udp.SendTo(from, snet.MakeMessage(context, &sproto.NodeStoreOpOk{}))
			case *sproto.NotifySlotTransIn:
				notify := msg.(*sproto.NotifySlotTransIn)
				fmt.Println("on slot trans in", notify.Slot, n.nodeId)
				n.udp.SendTo(from, snet.MakeMessage(context, &sproto.SlotTransInOk{
					Slot: notify.Slot,
				}))
			case *sproto.NotifySlotTransOut:
				notify := msg.(*sproto.NotifySlotTransOut)
				fmt.Println("on slot trans out", notify.Slot, n.nodeId)
				n.udp.SendTo(from, snet.MakeMessage(context, &sproto.SlotTransOutOk{
					Slot: notify.Slot,
				}))
			case *sproto.NotifyUpdateMeta:
				notify := msg.(*sproto.NotifyUpdateMeta)
				n.metaVersion = notify.Version
			case *sproto.IsTransInReady:
				fmt.Println("on IsTransInReady")
				n.udp.SendTo(from, snet.MakeMessage(context, &sproto.IsTransInReadyResp{
					Ready: true,
					Slot:  msg.(*sproto.IsTransInReady).Slot,
				}))
			}
		}
	}
}

func (n *testKvnode) stop() {
	n.udp.Close()
}

func testSlotTransfer(t *testing.T, p *pd) {

	GetSugar().Infof("testSlotTransfer")

	//先添加一个set

	waitBalanceFinish := func() {
		waitCondition(func() bool {
			ret := make(chan bool, 1)
			p.mainque.AppendHighestPriotiryItem(func() {
				if len(p.pState.SlotTransfer) == 0 {
					for _, v := range p.pState.deployment.sets {
						for _, vv := range v.stores {
							fmt.Printf("store:%d slotCount:%d slots%v\n", vv.id, len(vv.slots.GetOpenBits()), vv.slots.GetOpenBits())
						}
					}
					ret <- true
				} else {
					ret <- false
				}
			})
			return <-ret
		})
	}

	conn, err := fnet.NewUdp("localhost:0", snet.Pack, snet.Unpack)
	assert.Nil(t, err)

	addr, _ := net.ResolveUDPAddr("udp", "localhost:8110")

	conn.SendTo(addr, snet.MakeMessage(0, &sproto.AddSet{
		&sproto.DeploymentSet{
			SetID: 2,
			Nodes: []*sproto.DeploymentKvnode{
				&sproto.DeploymentKvnode{
					NodeID:      2,
					Host:        "localhost",
					ServicePort: 9210,
					RaftPort:    9211,
				},
			},
		},
	}))

	recvbuff := make([]byte, 256)

	_, r, err := conn.ReadFrom(recvbuff)

	assert.Equal(t, true, r.(*snet.Message).Msg.(*sproto.AddSetResp).Ok)

	fmt.Println(r.(*snet.Message).Msg.(*sproto.AddSetResp).Reason)

	waitCondition(func() bool {
		ret := make(chan bool, 1)
		p.mainque.AppendHighestPriotiryItem(func() {
			if len(p.pState.deployment.sets) == 2 {
				ret <- true
			} else {
				ret <- false
			}
		})
		return <-ret
	})

	//启动两个节点
	node1 := &testKvnode{
		nodeId: 1,
	}

	node1.udp, _ = fnet.NewUdp("localhost:9110", snet.Pack, snet.Unpack)

	go node1.run()

	node2 := &testKvnode{
		nodeId: 2,
	}

	node2.udp, _ = fnet.NewUdp("localhost:9210", snet.Pack, snet.Unpack)

	go node2.run()

	fmt.Println("wait one")

	waitBalanceFinish()

	fmt.Println("mark")

	//将set 2 标记clear
	go func() {
		conn, err := fnet.NewUdp("localhost:0", snet.Pack, snet.Unpack)
		assert.Nil(t, err)

		addr, _ := net.ResolveUDPAddr("udp", "localhost:8110")

		conn.SendTo(addr, snet.MakeMessage(0, &sproto.SetMarkClear{
			SetID: 2,
		}))

		recvbuff := make([]byte, 256)

		_, r, err := conn.ReadFrom(recvbuff)

		assert.Equal(t, true, r.(*snet.Message).Msg.(*sproto.SetMarkClearResp).Ok)
	}()

	fmt.Println("wait")

	waitBalanceFinish()

	node1.stop()
	node2.stop()

}
