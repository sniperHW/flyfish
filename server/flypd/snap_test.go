package flypd

//go test -covermode=count -v -coverprofile=coverage.out -run=.
//go tool cover -html=coverage.out

import (
	"fmt"
	//"github.com/sniperHW/flyfish/logger"
	fnet "github.com/sniperHW/flyfish/pkg/net"
	snet "github.com/sniperHW/flyfish/server/net"
	sproto "github.com/sniperHW/flyfish/server/proto"
	//sslot "github.com/sniperHW/flyfish/server/slot"
	"github.com/stretchr/testify/assert"
	"net"
	"os"
	//"sync"
	"github.com/sniperHW/flyfish/logger"
	"github.com/sniperHW/flyfish/pkg/bitmap"
	"testing"
	"time"
)

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

func TestInstallDeployment(t *testing.T) {

	os.RemoveAll("./log/pd-1-1")
	os.RemoveAll("./log/pd-1-1-snap")

	var configStr string = `

	MainQueueMaxSize = 1000

	[Log]
		MaxLogfileSize  = 104857600 # 100mb
		LogDir          = "log"
		LogPrefix       = "gate"
		LogLevel        = "info"
		EnableLogStdout = false		

`

	l := logger.NewZapLogger("testPd.log", "./log", "Debug", 100, 14, true)
	InitLogger(l)

	conf, _ := LoadConfigStr(configStr)

	p, err := NewPd(conf, "localhost:8110", 1, "1@http:\\localhost:8110")

	for {
		if p.isLeader() && p.ready {
			break
		} else {
			time.Sleep(time.Second)
		}
	}

	conn, err := fnet.NewUdp("localhost:0", snet.Pack, snet.Unpack)
	assert.Nil(t, err)

	addr, _ := net.ResolveUDPAddr("udp", "localhost:8110")

	KvNodePerSet = 3

	install := &sproto.InstallDeployment{}
	set1 := &sproto.DeploymentSet{SetID: 1}
	set1.Nodes = append(set1.Nodes, &sproto.DeploymentKvnode{
		NodeID:      1,
		Host:        "192.168.0.1",
		ServicePort: 8110,
		InterPort:   8111,
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

	/*set1.Nodes = append(set1.Nodes, &sproto.DeploymentKvnode{
		NodeID:      1,
		Host:        "192.168.0.1",
		ServicePort: 8110,
		InterPort:   8111,
	})

	set1.Nodes = append(set1.Nodes, &sproto.DeploymentKvnode{
		NodeID:      1,
		Host:        "192.168.0.1",
		ServicePort: 8110,
		InterPort:   8111,
	})*/

	p.Stop()

}
