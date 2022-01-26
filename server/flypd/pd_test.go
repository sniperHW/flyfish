package flypd

//go test -covermode=count -v -coverprofile=coverage.out -run=.
//go tool cover -html=coverage.out

import (
	"fmt"
	"github.com/sniperHW/flyfish/db"
	"github.com/sniperHW/flyfish/db/sql"
	"github.com/sniperHW/flyfish/logger"
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

	p, _ := NewPd(1, 1, 1, false, conf, "localhost:8110", "1@1@http://localhost:18110@")

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

	p, _ = NewPd(1, 1, 1, false, conf, "localhost:8110", "1@1@http://localhost:18110@")

	for {
		if p.isLeader() {
			break
		} else {
			time.Sleep(time.Second)
		}
	}

	fmt.Printf("instanceCounter:%d\n", p.pState.deployment.instanceCounter)

	p.Stop()

}

func TestAddRemovePd(t *testing.T) {
	os.RemoveAll("./raftLog")

	l := logger.NewZapLogger("testPd.log", "./log", "Debug", 100, 14, 10, true)
	InitLogger(l)

	conf, _ := LoadConfigStr(configStr)

	fmt.Println("conf.DBType", conf.DBType)

	p1, _ := NewPd(1, 1, 1, false, conf, "localhost:8110", "1@1@http://localhost:18110@")

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

	var instanceID uint32
	var raftCluster string
	var cluster uint16

	{
		conn.SendTo(addr, snet.MakeMessage(0, &sproto.AddPdNode{
			Id:  2,
			Url: "http://localhost:18111",
		}))

		recvbuff := make([]byte, 256)
		_, r, _ := conn.ReadFrom(recvbuff)

		ret := r.(*snet.Message).Msg.(*sproto.AddPdNodeResp)

		instanceID = ret.InstanceID
		raftCluster = ret.RaftCluster
		cluster = uint16(ret.Cluster)
	}

	fmt.Println(raftCluster)

	p2, err := NewPd(2, cluster, instanceID, true, conf, "localhost:8111", raftCluster)

	time.Sleep(time.Second)

	for {
		if resp, err := consoleUdp.Call([]string{"localhost:8110", "localhost:8111"}, &sproto.RemovePdNode{
			Id:         2,
			InstanceID: instanceID,
		}, time.Second); nil != resp && resp.(*sproto.RemovePdNodeResp).Reason == "membership: ID not found" {
			break
		} else {
			fmt.Println(err)
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

	p, _ := NewPd(1, 1, 1, false, conf, "localhost:8110", "1@1@http://localhost:18110@")

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
		if len(p.pState.deployment.sets[1].nodes) == 1 {
			return true
		} else {
			return false
		}
	})

	node1.stop()

	conn.Close()
}

func testAddRemSet(t *testing.T, p *pd) {
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
		if len(p.pState.deployment.sets) == 2 {
			return true
		} else {
			return false
		}
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
		if len(p.pState.deployment.sets) == 1 {
			return true
		} else {
			conn.SendTo(addr, snet.MakeMessage(0, &sproto.RemSet{
				SetID: 2,
			}))
			_, r, err = conn.ReadFrom(recvbuff)
			return false
		}
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
				fmt.Println("on NotifyUpdateMeta", notify.Version)
			case *sproto.IsTransInReady:
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

	fmt.Println("testSlotTransfer")

	//先添加一个set

	ch := make(chan struct{})

	p.onBalanceFinish = func() {
		for _, v := range p.pState.deployment.sets {
			for _, vv := range v.stores {
				fmt.Printf("store:%d slotCount:%d slots%v\n", vv.id, len(vv.slots.GetOpenBits()), vv.slots.GetOpenBits())
			}
		}
		ch <- struct{}{}
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
		if len(p.pState.deployment.sets) == 2 {
			return true
		} else {
			return false
		}
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

	<-ch

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

	<-ch

	node1.stop()
	node2.stop()

}

/*
func makeInstallDeployment(setCount int) *sproto.InstallDeployment {
	nodeID := 1
	servicePort := 1
	raftPort := setCount*MinReplicaPerSet + 1

	install := &sproto.InstallDeployment{}

	for i := 0; i < setCount; i++ {
		set := &sproto.DeploymentSet{SetID: int32(i + 1)}
		for j := 0; j < MinReplicaPerSet; j++ {
			set.Nodes = append(set.Nodes, &sproto.DeploymentKvnode{
				NodeID:      int32(nodeID),
				Host:        "localhost",
				ServicePort: int32(servicePort),
				RaftPort:    int32(raftPort),
			})
			nodeID++
			servicePort++
			raftPort++
		}

		install.Sets = append(install.Sets, set)
	}

	return install
}

func TestRouteInfo(t *testing.T) {
	sslot.SlotCount = 16384
	MinReplicaPerSet = 5
	for i := 1; i <= 100; i++ {
		install := makeInstallDeployment(i)

		d := &deployment{}

		if err := d.loadFromPB(install.Sets); nil != err {
			t.Fatal(err)
		}

		r := d.queryRouteInfo(&sproto.QueryRouteInfo{Version: 0})

		b, err := snet.Pack(snet.MakeMessage(0, r))

		if nil != err {
			t.Fatal(err)
		}

		fmt.Println(i, len(b))

		_, err = snet.Unpack(b)

		if nil != err {
			t.Fatal(err)
		}
	}

	{
		install := makeInstallDeployment(1)

		d := &deployment{}

		if err := d.loadFromPB(install.Sets); nil != err {
			t.Fatal(err)
		}

		//增加一个set
		s := &set{
			id:     2,
			nodes:  map[int]*kvnode{},
			stores: map[int]*store{},
		}

		s.nodes[100] = &kvnode{
			id:          100,
			host:        "localhost",
			servicePort: 1000,
			raftPort:    1001,
			set:         s,
		}

		s.stores[100] = &store{
			id:    100,
			set:   s,
			slots: bitmap.New(sslot.SlotCount),
		}

		d.sets[2] = s
		d.version++
		s.version = d.version

		r := d.queryRouteInfo(&sproto.QueryRouteInfo{Version: 1})

		assert.Equal(t, 1, len(r.Sets))

		assert.Equal(t, 0, len(r.RemoveSets))

		assert.Equal(t, int32(100), r.Sets[0].Kvnodes[0].NodeID)

	}

	{
		install := makeInstallDeployment(5)

		d := &deployment{}

		if err := d.loadFromPB(install.Sets); nil != err {
			t.Fatal(err)
		}

		//移除两个节点
		delete(d.sets, 2)
		delete(d.sets, 4)

		//增加一个set
		s := &set{
			id:     6,
			nodes:  map[int]*kvnode{},
			stores: map[int]*store{},
		}

		s.nodes[100] = &kvnode{
			id:          100,
			host:        "localhost",
			servicePort: 1000,
			raftPort:    1001,
			set:         s,
		}

		s.stores[100] = &store{
			id:    100,
			set:   s,
			slots: bitmap.New(sslot.SlotCount),
		}

		d.sets[6] = s
		d.version++
		s.version = d.version

		r := d.queryRouteInfo(&sproto.QueryRouteInfo{Version: 1, Sets: []int32{1, 2, 3, 4, 5}})

		assert.Equal(t, 1, len(r.Sets))

		fmt.Println(r.RemoveSets)

		assert.Equal(t, 2, len(r.RemoveSets))

		assert.Equal(t, int32(100), r.Sets[0].Kvnodes[0].NodeID)

	}
}*/
