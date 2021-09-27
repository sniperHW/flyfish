package flypd

//go test -covermode=count -v -coverprofile=coverage.out -run=.
//go tool cover -html=coverage.out

import (
	"fmt"
	//"github.com/sniperHW/flyfish/logger"
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
		interPort:   8111,
		set:         set1,
	}

	set1.nodes[2] = &kvnode{
		id:          2,
		host:        "192.168.0.2",
		servicePort: 8110,
		interPort:   8111,
		set:         set1,
	}

	set1.nodes[3] = &kvnode{
		id:          3,
		host:        "192.168.0.3",
		servicePort: 8110,
		interPort:   8111,
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
			InterPort:   8111,
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

	os.RemoveAll("./log/pd-1-1")
	os.RemoveAll("./log/pd-1-1-snap")

	var configStr string = `

	MainQueueMaxSize = 1000

	[Log]
		MaxLogfileSize  = 104857600 # 100mb
		LogDir          = "log"
		LogPrefix       = "pd"
		LogLevel        = "info"
		EnableLogStdout = false		

`

	l := logger.NewZapLogger("testPd.log", "./log", "Debug", 100, 14, true)
	InitLogger(l)

	conf, _ := LoadConfigStr(configStr)

	p, _ := NewPd(conf, "localhost:8110", 1, "1@http://localhost:8110")

	for {
		if p.isLeader() && p.ready {
			break
		} else {
			time.Sleep(time.Second)
		}
	}

	testInstallDeployment(t, p)

	testAddRemNode(t, p)

	p.Stop()
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
		InterPort:   9111,
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

	node1.udp, _ = fnet.NewUdp("localhost:9111", snet.Pack, snet.Unpack)

	go node1.run()

	conn, err := fnet.NewUdp("localhost:0", snet.Pack, snet.Unpack)
	assert.Nil(t, err)

	addr, _ := net.ResolveUDPAddr("udp", "localhost:8110")

	conn.SendTo(addr, &sproto.AddNode{
		SetID:       1,
		NodeID:      2,
		Host:        "localhost",
		ServicePort: 9120,
		InterPort:   9121,
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

func testAddSet(t *testing.T, p *pd) {

}
