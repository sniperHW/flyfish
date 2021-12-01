package flypd

//go test -covermode=count -v -coverprofile=coverage.out -run=.
//go tool cover -html=coverage.out

import (
	"fmt"
	//"github.com/sniperHW/flyfish/logger"
	"github.com/sniperHW/flyfish/logger"
	"github.com/sniperHW/flyfish/pkg/bitmap"
	//"github.com/sniperHW/flyfish/pkg/compress"
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
		deployment:   d,
		addingNode:   map[int]*AddingNode{},
		removingNode: map[int]*RemovingNode{},
		slotTransfer: map[int]*TransSlotTransfer{},
	}

	p1.addingNode[11] = &AddingNode{
		KvNodeJson: KvNodeJson{
			NodeID:      11,
			Host:        "192.168.0.11",
			ServicePort: 8110,
			RaftPort:    8111,
		},
		SetID: 1,
	}

	p1.removingNode[3] = &RemovingNode{
		NodeID: 3,
		SetID:  1,
	}

	p1.slotTransfer[2] = &TransSlotTransfer{
		Slot:             2,
		SetOut:           1,
		StoreTransferOut: 1,
		SetIn:            2,
		StoreTransferIn:  3,
	}

	bytes, err := p1.getSnapshot()

	fmt.Println(len(bytes))

	assert.Nil(t, err)

	p2 := &pd{
		addingNode:   map[int]*AddingNode{},
		removingNode: map[int]*RemovingNode{},
		slotTransfer: map[int]*TransSlotTransfer{},
	}

	err = p2.recoverFromSnapshot(bytes)

	assert.Nil(t, err)

	assert.Equal(t, 1, len(p2.deployment.sets))

	assert.Equal(t, p2.deployment.sets[1].nodes[2].host, "192.168.0.2")

	assert.Equal(t, true, p2.deployment.sets[1].stores[2].slots.Test(11))

	assert.Equal(t, false, p2.deployment.sets[1].stores[2].slots.Test(0))

	assert.Equal(t, 1, p2.addingNode[11].SetID)

	assert.Equal(t, 3, p2.removingNode[3].NodeID)

	assert.Equal(t, 2, p2.slotTransfer[2].Slot)

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

	p := NewPd(1, conf, "localhost:8110", "1@http://localhost:8110")

	p.Start()

	for {
		if p.isLeader() && p.ready {
			break
		} else {
			time.Sleep(time.Second)
		}
	}

	testInstallDeployment(t, p)

	testAddRemNode(t, p)

	testAddRemSet(t, p)

	testSlotTransfer(t, p)

	testFlygate(t, p)

	p.Stop()

	p = NewPd(1, conf, "localhost:8110", "1@http://localhost:8110")

	p.Start()

	for {
		if p.isLeader() && p.ready {
			break
		} else {
			time.Sleep(time.Second)
		}
	}

	p.Stop()

}

func testFlygate(t *testing.T, p *pd) {
	conn, err := fnet.NewUdp("localhost:0", snet.Pack, snet.Unpack)
	assert.Nil(t, err)
	addr, _ := net.ResolveUDPAddr("udp", "localhost:8110")

	conn.SendTo(addr, &sproto.QueryRouteInfo{Version: 0, Service: "localhost:8119"})
	_, r, err := conn.ReadFrom(make([]byte, 65535))

	assert.Equal(t, r.(*sproto.QueryRouteInfoResp).Version, p.deployment.version)

	conn.SendTo(addr, &sproto.GetFlyGate{})
	_, r, err = conn.ReadFrom(make([]byte, 65535))

	assert.Equal(t, r.(*sproto.GetFlyGateResp).GateService, "localhost:8119")

}

func testInstallDeployment(t *testing.T, p *pd) {

	conn, err := fnet.NewUdp("localhost:0", snet.Pack, snet.Unpack)
	assert.Nil(t, err)

	addr, _ := net.ResolveUDPAddr("udp", "localhost:8110")

	KvNodePerSet = 3

	install := &sproto.InstallDeployment{}
	set1 := &sproto.DeploymentSet{SetID: 1}
	set1.Nodes = append(set1.Nodes, &sproto.DeploymentKvnode{
		NodeID:      1,
		Host:        "localhost",
		ServicePort: 9110,
		RaftPort:    9111,
	})
	install.Sets = append(install.Sets, set1)

	conn.SendTo(addr, install)
	recvbuff := make([]byte, 256)
	_, r, err := conn.ReadFrom(recvbuff)

	assert.Equal(t, r.(*sproto.InstallDeploymentResp).Ok, false)

	assert.Equal(t, r.(*sproto.InstallDeploymentResp).Reason, "node count of set should be 3")

	KvNodePerSet = 1

	conn.SendTo(addr, install)

	_, r, err = conn.ReadFrom(recvbuff)

	assert.Equal(t, r.(*sproto.InstallDeploymentResp).Ok, true)

	conn.Close()

}

type testKvnode struct {
	udp    *fnet.Udp
	nodeId int
}

func (n *testKvnode) run() {
	recvbuff := make([]byte, 64*1024)
	for {
		from, msg, err := n.udp.ReadFrom(recvbuff)
		if nil != err {
			return
		} else {
			fmt.Println(msg)
			switch msg.(type) {
			case *sproto.NotifyAddNode:
				fmt.Println("on NotifyAddNode")
				notify := msg.(*sproto.NotifyAddNode)
				for _, v := range notify.Stores {
					n.udp.SendTo(from, &sproto.NotifyAddNodeResp{
						NodeID: notify.NodeID,
						Store:  v,
					})
				}
			case *sproto.NotifyRemNode:
				fmt.Println("on NotifyRemNode")
				notify := msg.(*sproto.NotifyRemNode)
				for _, v := range notify.Stores {
					n.udp.SendTo(from, &sproto.NotifyRemNodeResp{
						NodeID: notify.NodeID,
						Store:  v,
					})
				}
			case *sproto.NotifySlotTransIn:
				notify := msg.(*sproto.NotifySlotTransIn)
				n.udp.SendTo(from, &sproto.NotifySlotTransInResp{
					Slot: notify.Slot,
				})
			case *sproto.NotifySlotTransOut:
				notify := msg.(*sproto.NotifySlotTransOut)
				n.udp.SendTo(from, &sproto.NotifySlotTransOutResp{
					Slot: notify.Slot,
				})
			}
		}
	}
}

func (n *testKvnode) stop() {
	n.udp.Close()
}

func testAddRemNode(t *testing.T, p *pd) {

	node1 := &testKvnode{
		nodeId: 1,
	}

	node1.udp, _ = fnet.NewUdp("localhost:9110", snet.Pack, snet.Unpack)

	go node1.run()

	conn, err := fnet.NewUdp("localhost:0", snet.Pack, snet.Unpack)
	assert.Nil(t, err)

	addr, _ := net.ResolveUDPAddr("udp", "localhost:8110")

	conn.SendTo(addr, &sproto.AddNode{
		SetID:       1,
		NodeID:      2,
		Host:        "localhost",
		ServicePort: 9120,
		RaftPort:    9121,
	})

	recvbuff := make([]byte, 256)

	_, r, err := conn.ReadFrom(recvbuff)

	assert.Equal(t, true, r.(*sproto.AddNodeResp).Ok)

	waitCondition(func() bool {
		if len(p.deployment.sets[1].nodes) == 2 {
			return true
		} else {
			return false
		}
	})

	conn.SendTo(addr, &sproto.RemNode{
		SetID:  1,
		NodeID: 2,
	})

	_, r, err = conn.ReadFrom(recvbuff)

	assert.Equal(t, true, r.(*sproto.RemNodeResp).Ok)

	waitCondition(func() bool {
		if len(p.deployment.sets[1].nodes) == 1 {
			return true
		} else {
			return false
		}
	})

	conn.Close()

	node1.stop()

}

func testAddRemSet(t *testing.T, p *pd) {

	conn, err := fnet.NewUdp("localhost:0", snet.Pack, snet.Unpack)
	assert.Nil(t, err)

	addr, _ := net.ResolveUDPAddr("udp", "localhost:8110")

	conn.SendTo(addr, &sproto.AddSet{
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
	})

	recvbuff := make([]byte, 256)

	_, r, err := conn.ReadFrom(recvbuff)

	assert.Equal(t, true, r.(*sproto.AddSetResp).Ok)

	waitCondition(func() bool {
		if len(p.deployment.sets) == 2 {
			return true
		} else {
			return false
		}
	})

	assert.Equal(t, 3, int(p.deployment.sets[2].nodes[3].id))

	conn.SendTo(addr, &sproto.RemSet{
		SetID: 2,
	})

	_, r, err = conn.ReadFrom(recvbuff)

	assert.Equal(t, true, r.(*sproto.RemSetResp).Ok)

	waitCondition(func() bool {
		if len(p.deployment.sets) == 1 {
			return true
		} else {
			return false
		}
	})

}

func testSlotTransfer(t *testing.T, p *pd) {

	fmt.Println("testSlotTransfer")

	//先添加一个set

	ch := make(chan struct{})

	p.onBalanceFinish = func() {
		for _, v := range p.deployment.sets {
			for _, vv := range v.stores {
				fmt.Printf("store:%d slotCount:%d slots%v\n", vv.id, len(vv.slots.GetOpenBits()), vv.slots.GetOpenBits())
			}
		}
		ch <- struct{}{}
	}

	conn, err := fnet.NewUdp("localhost:0", snet.Pack, snet.Unpack)
	assert.Nil(t, err)

	addr, _ := net.ResolveUDPAddr("udp", "localhost:8110")

	conn.SendTo(addr, &sproto.AddSet{
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
	})

	recvbuff := make([]byte, 256)

	_, r, err := conn.ReadFrom(recvbuff)

	assert.Equal(t, true, r.(*sproto.AddSetResp).Ok)

	waitCondition(func() bool {
		if len(p.deployment.sets) == 2 {
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

		conn.SendTo(addr, &sproto.SetMarkClear{
			SetID: 2,
		})

		recvbuff := make([]byte, 256)

		_, r, err := conn.ReadFrom(recvbuff)

		assert.Equal(t, true, r.(*sproto.SetMarkClearResp).Ok)
	}()

	<-ch

	node1.stop()
	node2.stop()

}

func makeInstallDeployment(setCount int) *sproto.InstallDeployment {
	nodeID := 1
	servicePort := 1
	raftPort := setCount*KvNodePerSet + 1

	install := &sproto.InstallDeployment{}

	for i := 0; i < setCount; i++ {
		set := &sproto.DeploymentSet{SetID: int32(i + 1)}
		for j := 0; j < KvNodePerSet; j++ {
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
	KvNodePerSet = 5
	for i := 1; i <= 100; i++ {
		install := makeInstallDeployment(i)

		d := &deployment{}

		if err := d.loadFromPB(install.Sets); nil != err {
			t.Fatal(err)
		}

		r := d.queryRouteInfo(&sproto.QueryRouteInfo{Version: 0})

		b, err := snet.Pack(r)

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
