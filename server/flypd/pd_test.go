package flypd

//go test -covermode=count -v -coverprofile=coverage.out -run=.
//go tool cover -html=coverage.out

import (
	"fmt"
	//"github.com/sniperHW/flyfish/logger"
	"github.com/sniperHW/flyfish/db"
	"github.com/sniperHW/flyfish/logger"
	"github.com/sniperHW/flyfish/pkg/bitmap"
	fnet "github.com/sniperHW/flyfish/pkg/net"
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

func genMeta() *db.DbDef {
	m := db.DbDef{}

	t1 := db.TableDef{
		Name: "Table1",
	}

	{
		field1 := db.FieldDef{
			Name:        "field1",
			Type:        "int",
			DefautValue: "1",
		}

		field2 := db.FieldDef{
			Name:        "field2",
			Type:        "float",
			DefautValue: "1.2",
		}

		field3 := db.FieldDef{
			Name:        "field3",
			Type:        "string",
			DefautValue: "hello",
		}

		field4 := db.FieldDef{
			Name: "field4",
			Type: "string",
		}

		field5 := db.FieldDef{
			Name: "field5",
			Type: "blob",
		}

		t1.Fields = append(t1.Fields, &field1)
		t1.Fields = append(t1.Fields, &field2)
		t1.Fields = append(t1.Fields, &field3)
		t1.Fields = append(t1.Fields, &field4)
		t1.Fields = append(t1.Fields, &field5)
	}

	m.TableDefs = append(m.TableDefs, &t1)

	return &m
}

func TestSnapShot(t *testing.T) {

	d := &deployment{
		sets: map[int]*set{},
	}

	set1 := &set{
		id:     1,
		nodes:  map[int]*kvnode{},
		stores: map[int]*store{},
	}

	set1.nodes[1] = &kvnode{
		id:          1,
		host:        "192.168.0.1",
		servicePort: 8110,
		raftPort:    8111,
		set:         set1,
	}

	set1.nodes[2] = &kvnode{
		id:          2,
		host:        "192.168.0.2",
		servicePort: 8110,
		raftPort:    8111,
		set:         set1,
	}

	set1.nodes[3] = &kvnode{
		id:          3,
		host:        "192.168.0.3",
		servicePort: 8110,
		raftPort:    8111,
		set:         set1,
	}

	slot1 := bitmap.New(30)
	for i := 0; i < 9; i++ {
		slot1.Set(i)
	}

	set1.stores[1] = &store{
		id:    1,
		slots: slot1,
		set:   set1,
	}

	slot2 := bitmap.New(30)
	for i := 10; i < 19; i++ {
		slot2.Set(i)
	}

	set1.stores[2] = &store{
		id:    2,
		slots: slot2,
		set:   set1,
	}

	slot3 := bitmap.New(30)
	for i := 20; i < 29; i++ {
		slot3.Set(i)
	}

	set1.stores[3] = &store{
		id:    3,
		slots: slot3,
		set:   set1,
	}

	d.sets[1] = set1

	p1 := &pd{
		pState: persistenceState{
			deployment:   d,
			SlotTransfer: map[int]*TransSlotTransfer{},
		},
		markClearSet: map[int]*set{},
	}

	p1.pState.SlotTransfer[2] = &TransSlotTransfer{
		Slot:             2,
		SetOut:           1,
		StoreTransferOut: 1,
		SetIn:            2,
		StoreTransferIn:  3,
	}

	{
		m := genMeta()
		p1.pState.Meta.Version = 1
		p1.pState.Meta.MetaDef = m
		p1.pState.Meta.MetaBytes, _ = db.DbDefToJsonString(m)
		p1.pState.MetaTransaction = &MetaTransaction{
			MetaDef: m,
			Store:   []MetaTransactionStore{MetaTransactionStore{StoreID: 1, Ok: true}},
			Version: 2,
		}
	}

	bytes, err := p1.getSnapshot()

	fmt.Println(len(bytes))

	assert.Nil(t, err)

	p2 := &pd{
		pState: persistenceState{
			deployment:   &deployment{},
			SlotTransfer: map[int]*TransSlotTransfer{},
		},
		markClearSet: map[int]*set{},
	}

	err = p2.recoverFromSnapshot(bytes)

	assert.Nil(t, err)

	assert.Equal(t, p2.pState.deployment.sets[1].nodes[2].host, "192.168.0.2")

	fmt.Println(p2.pState.deployment.sets[1])

	assert.Equal(t, true, p2.pState.deployment.sets[1].stores[2].slots.Test(11))

	assert.Equal(t, false, p2.pState.deployment.sets[1].stores[2].slots.Test(0))

	assert.Equal(t, 2, p2.pState.SlotTransfer[2].Slot)

	assert.Equal(t, p2.pState.Meta.Version, int64(1))
	assert.Equal(t, p2.pState.MetaTransaction.Version, int64(2))

	assert.Equal(t, p2.pState.MetaTransaction.Store[0].StoreID, 1)
	assert.Equal(t, p2.pState.MetaTransaction.Store[0].Ok, true)

	assert.Equal(t, len(p2.pState.Meta.MetaBytes), len(p1.pState.Meta.MetaBytes))

}

func TestPd(t *testing.T) {

	sslot.SlotCount = 128

	os.RemoveAll("./raftLog")
	os.RemoveAll("./raftLog")

	var configStr string = `

	MainQueueMaxSize = 1000
	RaftLogDir       = "raftLog"
	RaftLogPrefix    = "pd"
	[Log]
		MaxLogfileSize  = 104857600 # 100mb
		LogDir          = "log"
		LogPrefix       = "pd"
		LogLevel        = "info"
		EnableLogStdout = false	
		MaxAge          = 14
		MaxBackups      = 10			

`

	l := logger.NewZapLogger("testPd.log", "./log", "Debug", 100, 14, 10, true)
	InitLogger(l)

	conf, _ := LoadConfigStr(configStr)

	p, _ := NewPd(1, false, conf, "localhost:8110", "1@http://localhost:8110@", nil)

	for {
		if p.isLeader() && p.ready {
			break
		} else {
			time.Sleep(time.Second)
		}
	}

	testSetMeta(t, p)

	testUpdateMeta1(t, p)

	testInstallDeployment(t, p)

	testAddRemNode(t, p)

	testAddRemSet(t, p)

	testUpdateMeta2(t, p)

	//testAddRemNode(t, p)

	testSlotTransfer(t, p)

	p.Stop()

	p, _ = NewPd(1, false, conf, "localhost:8110", "1@http://localhost:8110@", nil)

	for {
		if p.isLeader() && p.ready {
			break
		} else {
			time.Sleep(time.Second)
		}
	}

	p.Stop()

}

func testInstallDeployment(t *testing.T, p *pd) {

	conn, err := fnet.NewUdp("localhost:0", snet.Pack, snet.Unpack)
	assert.Nil(t, err)

	addr, _ := net.ResolveUDPAddr("udp", "localhost:8110")

	MinReplicaPerSet = 3

	install := &sproto.InstallDeployment{}
	set1 := &sproto.DeploymentSet{SetID: 1}
	set1.Nodes = append(set1.Nodes, &sproto.DeploymentKvnode{
		NodeID:      1,
		Host:        "localhost",
		ServicePort: 9110,
		RaftPort:    9111,
	})
	install.Sets = append(install.Sets, set1)

	conn.SendTo(addr, snet.MakeMessage(0, install))
	recvbuff := make([]byte, 4096)
	_, r, err := conn.ReadFrom(recvbuff)

	assert.Equal(t, r.(*snet.Message).Msg.(*sproto.InstallDeploymentResp).Ok, false)

	assert.Equal(t, r.(*snet.Message).Msg.(*sproto.InstallDeploymentResp).Reason, "node count of set should be 3")

	MinReplicaPerSet = 1

	conn.SendTo(addr, snet.MakeMessage(0, install))

	_, r, err = conn.ReadFrom(recvbuff)

	assert.Equal(t, r.(*snet.Message).Msg.(*sproto.InstallDeploymentResp).Ok, true)

	conn.SendTo(addr, snet.MakeMessage(0, &sproto.KvnodeBoot{
		NodeID: 1,
	}))

	_, r, err = conn.ReadFrom(recvbuff)

	assert.Equal(t, r.(*snet.Message).Msg.(*sproto.KvnodeBootResp).Ok, true)
	assert.Equal(t, r.(*snet.Message).Msg.(*sproto.KvnodeBootResp).ServiceHost, "localhost")

	conn.Close()

}

func testSetMeta(t *testing.T, p *pd) {
	m := genMeta()
	b, _ := db.DbDefToJsonString(m)

	conn, err := fnet.NewUdp("localhost:0", snet.Pack, snet.Unpack)
	assert.Nil(t, err)

	addr, _ := net.ResolveUDPAddr("udp", "localhost:8110")

	conn.SendTo(addr, snet.MakeMessage(0, &sproto.SetMeta{
		Meta: b,
	}))
	recvbuff := make([]byte, 256)
	_, r, err := conn.ReadFrom(recvbuff)

	assert.Equal(t, r.(*snet.Message).Msg.(*sproto.SetMetaResp).Ok, true)

	conn.Close()
}

func testUpdateMeta1(t *testing.T, p *pd) {
	t1 := sproto.MetaTable{
		Name: "Table1",
	}

	t1.Fields = append(t1.Fields, &sproto.MetaFiled{
		Name:    "field6",
		Type:    "string",
		Default: "hello",
	})

	t2 := sproto.MetaTable{
		Name: "Table2",
	}

	t2.Fields = append(t2.Fields, &sproto.MetaFiled{
		Name:    "field1",
		Type:    "string",
		Default: "hello",
	})

	u := &sproto.UpdateMeta{Updates: []*sproto.MetaTable{&t1, &t2}}

	conn, err := fnet.NewUdp("localhost:0", snet.Pack, snet.Unpack)
	assert.Nil(t, err)

	addr, _ := net.ResolveUDPAddr("udp", "localhost:8110")

	conn.SendTo(addr, snet.MakeMessage(0, u))
	recvbuff := make([]byte, 256)
	_, r, err := conn.ReadFrom(recvbuff)

	assert.Equal(t, r.(*snet.Message).Msg.(*sproto.UpdateMetaResp).Ok, true)

	conn.Close()
}

func testUpdateMeta2(t *testing.T, p *pd) {

	node1 := &testKvnode{
		nodeId: 1,
	}

	node1.udp, _ = fnet.NewUdp("localhost:9110", snet.Pack, snet.Unpack)

	go node1.run()

	t1 := sproto.MetaTable{
		Name: "Table1",
	}

	t1.Fields = append(t1.Fields, &sproto.MetaFiled{
		Name:    "field7",
		Type:    "string",
		Default: "hello",
	})

	u := &sproto.UpdateMeta{Updates: []*sproto.MetaTable{&t1}}

	conn, err := fnet.NewUdp("localhost:0", snet.Pack, snet.Unpack)
	assert.Nil(t, err)

	addr, _ := net.ResolveUDPAddr("udp", "localhost:8110")

	conn.SendTo(addr, snet.MakeMessage(0, u))
	recvbuff := make([]byte, 256)
	_, r, err := conn.ReadFrom(recvbuff)

	assert.Equal(t, r.(*snet.Message).Msg.(*sproto.UpdateMetaResp).Ok, true)

	conn.Close()

	waitCondition(func() bool {
		if p.pState.MetaTransaction == nil {
			return true
		} else {
			return false
		}
	})

	node1.stop()

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

	waitCondition(func() bool {
		if len(p.pState.deployment.sets[1].nodes) == 2 {
			return true
		} else {
			return false
		}
	})

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

	conn.SendTo(addr, snet.MakeMessage(0, &sproto.RemNode{
		SetID:  1,
		NodeID: 2,
	}))

	_, r, err = conn.ReadFrom(recvbuff)

	assert.Equal(t, true, r.(*snet.Message).Msg.(*sproto.RemNodeResp).Ok)

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

	node1 := &testKvnode{
		nodeId: 1,
	}

	node1.udp, err = fnet.NewUdp("localhost:9110", snet.Pack, snet.Unpack)

	if nil != err {
		panic(err)
	}

	go node1.run()

	node3 := &testKvnode{
		nodeId: 3,
	}

	node3.udp, _ = fnet.NewUdp("localhost:8311", snet.Pack, snet.Unpack)

	go node3.run()

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
			conn.ReadFrom(recvbuff)
			return false
		}
	})

	node1.stop()

	node3.stop()
}

type testKvnode struct {
	udp    *fnet.Udp
	nodeId int
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
				n.udp.SendTo(from, snet.MakeMessage(context, &sproto.SlotTransInOk{
					Slot: notify.Slot,
				}))
			case *sproto.NotifySlotTransOut:
				notify := msg.(*sproto.NotifySlotTransOut)
				n.udp.SendTo(from, snet.MakeMessage(context, &sproto.SlotTransOutOk{
					Slot: notify.Slot,
				}))
			case *sproto.NotifyUpdateMeta:
				fmt.Println("on NotifyUpdateMeta")
				notify := msg.(*sproto.NotifyUpdateMeta)
				n.udp.SendTo(from, snet.MakeMessage(context, &sproto.StoreUpdateMetaOk{
					Store:   notify.Store,
					Version: notify.Version,
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
}
