package flypd

//go test -covermode=count -v -coverprofile=coverage.out -run=.
//go tool cover -html=coverage.out

import (
	"fmt"
	"github.com/sniperHW/flyfish/logger"
	flynet "github.com/sniperHW/flyfish/pkg/net"
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

var configStr string = `

MainQueueMaxSize = 1000

stores = [1,2] #初始store将平分所有slots

[kvnodes]

[[kvnodes.node]]

Id = 1
Service = "localhost:8110"
RaftService = "http://127.0.0.1:22370"
UdpService = "localhost:9110"
Stores = [1]

[[kvnodes.node]]

Id = 2
Service = "localhost:8111"
RaftService = "http://127.0.0.1:22371"
UdpService = "localhost:9111"
Stores = [2]


[Log]
MaxLogfileSize  = 104857600
LogDir          = "log"
LogPrefix       = "pd"
LogLevel        = "info"
EnableLogStdout = false	

`

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

type testKvnode struct {
	udp      *flynet.Udp
	nodeId   int
	isLeader int
}

func (n *testKvnode) run() {
	recvbuff := make([]byte, 64*1024)
	for {
		from, msg, err := n.udp.ReadFrom(recvbuff)
		if nil != err {
			return
		} else {
			switch msg.(type) {
			case *sproto.NotifyKvnodeStoreTrans:
				n.udp.SendTo(from, &sproto.NotifyKvnodeStoreTransResp{
					TransID:  msg.(*sproto.NotifyKvnodeStoreTrans).GetTransID(),
					NodeId:   int32(n.nodeId),
					IsLeader: int32(n.isLeader),
				})
			}
		}
	}
}

func testAddNode(t *testing.T, pdAddr *net.UDPAddr, c *flynet.Udp) {
	c.SendTo(pdAddr, &sproto.AddKvnode{
		Seqno:       1,
		NodeId:      3,
		Service:     "localhost:8112",
		RaftService: "http://127.0.0.1:22372",
		UdpService:  "localhost:9112",
	})

	_, resp, _ := c.ReadFrom(make([]byte, 65536))
	assert.Equal(t, true, resp.(*sproto.AddKvnodeResp).GetOk())

	c.SendTo(pdAddr, &sproto.AddKvnode{
		Seqno:       1,
		NodeId:      3,
		Service:     "localhost:8112",
		RaftService: "http://127.0.0.1:22372",
		UdpService:  "localhost:9112",
	})

	_, resp, _ = c.ReadFrom(make([]byte, 65536))
	assert.Equal(t, true, resp.(*sproto.AddKvnodeResp).GetOk())

	c.SendTo(pdAddr, &sproto.AddKvnode{
		Seqno:       1,
		NodeId:      4,
		Service:     "localhost:8112",
		RaftService: "http://127.0.0.1:22372",
		UdpService:  "localhost:9112",
	})

	_, resp, _ = c.ReadFrom(make([]byte, 65536))
	assert.Equal(t, false, resp.(*sproto.AddKvnodeResp).GetOk())

	c.SendTo(pdAddr, &sproto.AddKvnode{
		Seqno:       1,
		NodeId:      4,
		Service:     "8112",
		RaftService: "http://127.0.0.1:22372",
		UdpService:  "localhost:9112",
	})

	_, resp, _ = c.ReadFrom(make([]byte, 65536))
	assert.Equal(t, false, resp.(*sproto.AddKvnodeResp).GetOk())

	c.SendTo(pdAddr, &sproto.AddKvnode{
		Seqno:       1,
		NodeId:      4,
		Service:     "8112",
		RaftService: "22372",
		UdpService:  "localhost:9112",
	})

	_, resp, _ = c.ReadFrom(make([]byte, 65536))
	assert.Equal(t, false, resp.(*sproto.AddKvnodeResp).GetOk())

	//////////////////////////////////////////////////////////////////

	c.SendTo(pdAddr, &sproto.AddKvnode{
		Seqno:       1,
		NodeId:      4,
		Service:     "sniperHW",
		RaftService: "http://127.0.0.1:22373",
		UdpService:  "localhost:9113",
	})

	_, resp, _ = c.ReadFrom(make([]byte, 65536))
	assert.Equal(t, false, resp.(*sproto.AddKvnodeResp).GetOk())
	fmt.Println(resp.(*sproto.AddKvnodeResp).GetReason())

	c.SendTo(pdAddr, &sproto.AddKvnode{
		Seqno:       3,
		NodeId:      4,
		Service:     "localhost:8113",
		RaftService: "http://127.0.0.1:22373",
		UdpService:  "sniperHW",
	})

	_, resp, _ = c.ReadFrom(make([]byte, 65536))
	assert.Equal(t, false, resp.(*sproto.AddKvnodeResp).GetOk())
	fmt.Println(resp.(*sproto.AddKvnodeResp).GetReason())

}

func testRemNode(t *testing.T, pdAddr *net.UDPAddr, c *flynet.Udp) {
	c.SendTo(pdAddr, &sproto.RemKvnode{
		Seqno:  1,
		NodeId: 3,
	})

	_, resp, _ := c.ReadFrom(make([]byte, 65536))
	assert.Equal(t, true, resp.(*sproto.RemKvnodeResp).GetOk())

	c.SendTo(pdAddr, &sproto.RemKvnode{
		Seqno:  1,
		NodeId: 3,
	})

	_, resp, _ = c.ReadFrom(make([]byte, 65536))
	assert.Equal(t, true, resp.(*sproto.RemKvnodeResp).GetOk())

	c.SendTo(pdAddr, &sproto.RemKvnode{
		Seqno:  1,
		NodeId: 1,
	})

	_, resp, _ = c.ReadFrom(make([]byte, 65536))
	assert.Equal(t, false, resp.(*sproto.RemKvnodeResp).GetOk())
	fmt.Println(resp.(*sproto.RemKvnodeResp).GetReason())

}

func testAddStore(t *testing.T, pdAddr *net.UDPAddr, c *flynet.Udp) {
	c.SendTo(pdAddr, &sproto.AddStore{
		Seqno: 1,
		Id:    1,
	})

	_, resp, _ := c.ReadFrom(make([]byte, 65536))
	assert.Equal(t, true, resp.(*sproto.AddStoreResp).GetOk())

	c.SendTo(pdAddr, &sproto.AddStore{
		Seqno: 1,
		Id:    3,
	})

	_, resp, _ = c.ReadFrom(make([]byte, 65536))
	assert.Equal(t, true, resp.(*sproto.AddStoreResp).GetOk())

}

func testRemStore(t *testing.T, pdAddr *net.UDPAddr, c *flynet.Udp) {
	c.SendTo(pdAddr, &sproto.RemStore{
		Seqno: 1,
		Id:    3,
	})

	_, resp, _ := c.ReadFrom(make([]byte, 65536))
	assert.Equal(t, true, resp.(*sproto.RemStoreResp).GetOk())

	c.SendTo(pdAddr, &sproto.RemStore{
		Seqno: 1,
		Id:    3,
	})

	_, resp, _ = c.ReadFrom(make([]byte, 65536))
	assert.Equal(t, true, resp.(*sproto.RemStoreResp).GetOk())

	c.SendTo(pdAddr, &sproto.RemStore{
		Seqno: 1,
		Id:    1,
	})

	_, resp, _ = c.ReadFrom(make([]byte, 65536))
	assert.Equal(t, false, resp.(*sproto.RemStoreResp).GetOk())
	fmt.Println(resp.(*sproto.RemStoreResp).GetReason())

}

func testKvnodeAddStore(t *testing.T, pdAddr *net.UDPAddr, c *flynet.Udp) {
	node1 := &testKvnode{
		nodeId:   1,
		isLeader: 1,
	}
	node1.udp, _ = flynet.NewUdp("localhost:9110", snet.Pack, snet.Unpack)

	node3 := &testKvnode{
		nodeId:   3,
		isLeader: 0,
	}
	node3.udp, _ = flynet.NewUdp("localhost:9112", snet.Pack, snet.Unpack)

	c.SendTo(pdAddr, &sproto.AddKvnode{
		Seqno:       1,
		NodeId:      3,
		Service:     "localhost:8112",
		RaftService: "http://127.0.0.1:22372",
		UdpService:  "localhost:9112",
	})

	_, resp, _ := c.ReadFrom(make([]byte, 65536))
	assert.Equal(t, true, resp.(*sproto.AddKvnodeResp).GetOk())

	go node1.run()
	go node3.run()

	c.SendTo(pdAddr, &sproto.KvnodeAddStore{
		Seqno:   1,
		NodeId:  3,
		StoreId: 1,
	})

	_, resp, _ = c.ReadFrom(make([]byte, 65536))
	assert.Equal(t, true, resp.(*sproto.KvnodeAddStoreResp).GetOk())

	time.Sleep(time.Second)

	node1.udp.Close()
	node3.udp.Close()

}

func testKvnodeRemStore(t *testing.T, pdAddr *net.UDPAddr, c *flynet.Udp) {
	node1 := &testKvnode{
		nodeId:   1,
		isLeader: 1,
	}
	node1.udp, _ = flynet.NewUdp("localhost:9110", snet.Pack, snet.Unpack)

	go node1.run()

	c.SendTo(pdAddr, &sproto.KvnodeRemStore{
		Seqno:   1,
		NodeId:  3,
		StoreId: 1,
	})

	_, resp, _ := c.ReadFrom(make([]byte, 65536))
	assert.Equal(t, true, resp.(*sproto.KvnodeRemStoreResp).GetOk())

	time.Sleep(time.Second)

	node1.udp.Close()
}

func testSnapshot(t *testing.T, pd *pd) {
	wait := make(chan struct{})
	pd.mainque.AppendHighestPriotiryItem(func() {
		snap, err := pd.getSnapshot()
		assert.Nil(t, err)

		err = pd.recoverFromSnapshot(snap)
		assert.Nil(t, err)

		n := pd.kvnodes[1]
		assert.NotNil(t, n)
		assert.NotNil(t, n.stores[1])
		for i := 0; i < sslot.SlotCount; i++ {
			assert.NotNil(t, pd.slot2store[int32(i)])
		}

		close(wait)
	})
	<-wait
}

func Test1(t *testing.T) {
	err := LoadConfigStr(configStr)
	if nil != err {
		panic(err)
	}

	InitLogger(logger.NewZapLogger("testPd.log", "./log", GetConfig().Log.LogLevel, 100, 14, true))

	if false {
		//先删除所有kv文件
		os.RemoveAll("./log/pd-1-1")
		os.RemoveAll("./log/pd-1-1-snap")
	}

	pd, err := NewPd("localhost:8110", 1, "1@http://127.0.0.1:22378")
	if nil != err {
		panic(err)
	}

	waitCondition(func() bool {
		return pd.isLeader()
	})

	time.Sleep(time.Second * 5)

	pdAddr, _ := net.ResolveUDPAddr("udp", "localhost:8110")

	c, _ := flynet.NewUdp("localhost:10010", snet.Pack, snet.Unpack)

	c.SendTo(pdAddr, &sproto.KvnodeBoot{
		NodeID: 1,
	})

	_, resp, err := c.ReadFrom(make([]byte, 65536))
	assert.Equal(t, "localhost:8110", resp.(*sproto.KvnodeBootResp).GetService())
	assert.Equal(t, int32(1), resp.(*sproto.KvnodeBootResp).GetStores()[0].GetId())

	c.SendTo(pdAddr, &sproto.KvnodeBoot{
		NodeID: 2,
	})

	_, resp, err = c.ReadFrom(make([]byte, 65536))
	assert.Equal(t, "localhost:8111", resp.(*sproto.KvnodeBootResp).GetService())
	assert.Equal(t, int32(2), resp.(*sproto.KvnodeBootResp).GetStores()[0].GetId())

	c.SendTo(pdAddr, &sproto.KvnodeBoot{
		NodeID: 10,
	})

	_, resp, err = c.ReadFrom(make([]byte, 65536))
	assert.Equal(t, false, resp.(*sproto.KvnodeBootResp).GetOk())

	fmt.Println("testAddNode")
	testAddNode(t, pdAddr, c)
	fmt.Println("testRemNode")
	testRemNode(t, pdAddr, c)
	fmt.Println("testAddStore")
	testAddStore(t, pdAddr, c)
	fmt.Println("testRemStore")
	testRemStore(t, pdAddr, c)
	fmt.Println("testKvnodeAddStore")
	testKvnodeAddStore(t, pdAddr, c)
	fmt.Println("testKvnodeRemStore")
	testKvnodeRemStore(t, pdAddr, c)

	pd.Stop()

	fmt.Printf("start again")

	pd, err = NewPd("localhost:8110", 1, "1@http://127.0.0.1:22378")
	if nil != err {
		panic(err)
	}

	waitCondition(func() bool {
		return pd.isLeader()
	})

	wait := make(chan struct{})
	pd.mainque.AppendHighestPriotiryItem(func() {
		n := pd.kvnodes[1]
		assert.NotNil(t, n)
		assert.NotNil(t, n.stores[1])
		for i := 0; i < sslot.SlotCount; i++ {
			assert.NotNil(t, pd.slot2store[int32(i)])
		}
		close(wait)
	})
	<-wait

	testSnapshot(t, pd)

	pd.Stop()

}
