package flypd

//go test -covermode=count -v -coverprofile=coverage.out -run=.
//go tool cover -html=coverage.out

import (
	"fmt"
	"github.com/gogo/protobuf/proto"
	"github.com/sniperHW/flyfish/logger"
	pdnet "github.com/sniperHW/flyfish/server/pd/net"
	pdproto "github.com/sniperHW/flyfish/server/pd/proto"
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
	udp      *pdnet.Udp
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
			case *pdproto.NotifyKvnodeStoreTrans:
				n.udp.SendTo(from, &pdproto.NotifyKvnodeStoreTransResp{
					TransID:  proto.Int64(msg.(*pdproto.NotifyKvnodeStoreTrans).GetTransID()),
					NodeId:   proto.Int32(int32(n.nodeId)),
					IsLeader: proto.Int32(int32(n.isLeader)),
				})
			}
		}
	}
}

func testAddNode(t *testing.T, pdAddr *net.UDPAddr, c *pdnet.Udp) {
	c.SendTo(pdAddr, &pdproto.AddKvnode{
		Seqno:       proto.Int64(1),
		NodeId:      proto.Int32(3),
		Service:     proto.String("localhost:8112"),
		RaftService: proto.String("http://127.0.0.1:22372"),
		UdpService:  proto.String("localhost:9112"),
	})

	_, resp, _ := c.ReadFrom(make([]byte, 65536))
	assert.Equal(t, true, resp.(*pdproto.AddKvnodeResp).GetOk())

	c.SendTo(pdAddr, &pdproto.AddKvnode{
		Seqno:       proto.Int64(1),
		NodeId:      proto.Int32(3),
		Service:     proto.String("localhost:8112"),
		RaftService: proto.String("http://127.0.0.1:22372"),
		UdpService:  proto.String("localhost:9112"),
	})

	_, resp, _ = c.ReadFrom(make([]byte, 65536))
	assert.Equal(t, true, resp.(*pdproto.AddKvnodeResp).GetOk())

	c.SendTo(pdAddr, &pdproto.AddKvnode{
		Seqno:       proto.Int64(1),
		NodeId:      proto.Int32(4),
		Service:     proto.String("localhost:8112"),
		RaftService: proto.String("http://127.0.0.1:22372"),
		UdpService:  proto.String("localhost:9112"),
	})

	_, resp, _ = c.ReadFrom(make([]byte, 65536))
	assert.Equal(t, false, resp.(*pdproto.AddKvnodeResp).GetOk())

	c.SendTo(pdAddr, &pdproto.AddKvnode{
		Seqno:       proto.Int64(1),
		NodeId:      proto.Int32(4),
		Service:     proto.String("8112"),
		RaftService: proto.String("http://127.0.0.1:22372"),
		UdpService:  proto.String("localhost:9112"),
	})

	_, resp, _ = c.ReadFrom(make([]byte, 65536))
	assert.Equal(t, false, resp.(*pdproto.AddKvnodeResp).GetOk())

	c.SendTo(pdAddr, &pdproto.AddKvnode{
		Seqno:       proto.Int64(1),
		NodeId:      proto.Int32(4),
		Service:     proto.String("8112"),
		RaftService: proto.String("22372"),
		UdpService:  proto.String("localhost:9112"),
	})

	_, resp, _ = c.ReadFrom(make([]byte, 65536))
	assert.Equal(t, false, resp.(*pdproto.AddKvnodeResp).GetOk())

	//////////////////////////////////////////////////////////////////

	c.SendTo(pdAddr, &pdproto.AddKvnode{
		Seqno:       proto.Int64(1),
		NodeId:      proto.Int32(4),
		Service:     proto.String("sniperHW"),
		RaftService: proto.String("http://127.0.0.1:22373"),
		UdpService:  proto.String("localhost:9113"),
	})

	_, resp, _ = c.ReadFrom(make([]byte, 65536))
	assert.Equal(t, false, resp.(*pdproto.AddKvnodeResp).GetOk())
	fmt.Println(resp.(*pdproto.AddKvnodeResp).GetReason())

	c.SendTo(pdAddr, &pdproto.AddKvnode{
		Seqno:       proto.Int64(3),
		NodeId:      proto.Int32(4),
		Service:     proto.String("localhost:8113"),
		RaftService: proto.String("http://127.0.0.1:22373"),
		UdpService:  proto.String("sniperHW"),
	})

	_, resp, _ = c.ReadFrom(make([]byte, 65536))
	assert.Equal(t, false, resp.(*pdproto.AddKvnodeResp).GetOk())
	fmt.Println(resp.(*pdproto.AddKvnodeResp).GetReason())

}

func testRemNode(t *testing.T, pdAddr *net.UDPAddr, c *pdnet.Udp) {
	c.SendTo(pdAddr, &pdproto.RemKvnode{
		Seqno:  proto.Int64(1),
		NodeId: proto.Int32(3),
	})

	_, resp, _ := c.ReadFrom(make([]byte, 65536))
	assert.Equal(t, true, resp.(*pdproto.RemKvnodeResp).GetOk())

	c.SendTo(pdAddr, &pdproto.RemKvnode{
		Seqno:  proto.Int64(1),
		NodeId: proto.Int32(3),
	})

	_, resp, _ = c.ReadFrom(make([]byte, 65536))
	assert.Equal(t, true, resp.(*pdproto.RemKvnodeResp).GetOk())

	c.SendTo(pdAddr, &pdproto.RemKvnode{
		Seqno:  proto.Int64(1),
		NodeId: proto.Int32(1),
	})

	_, resp, _ = c.ReadFrom(make([]byte, 65536))
	assert.Equal(t, false, resp.(*pdproto.RemKvnodeResp).GetOk())
	fmt.Println(resp.(*pdproto.RemKvnodeResp).GetReason())

}

func testAddStore(t *testing.T, pdAddr *net.UDPAddr, c *pdnet.Udp) {
	c.SendTo(pdAddr, &pdproto.AddStore{
		Seqno: proto.Int64(1),
		Id:    proto.Int32(1),
	})

	_, resp, _ := c.ReadFrom(make([]byte, 65536))
	assert.Equal(t, true, resp.(*pdproto.AddStoreResp).GetOk())

	c.SendTo(pdAddr, &pdproto.AddStore{
		Seqno: proto.Int64(1),
		Id:    proto.Int32(3),
	})

	_, resp, _ = c.ReadFrom(make([]byte, 65536))
	assert.Equal(t, true, resp.(*pdproto.AddStoreResp).GetOk())

}

func testRemStore(t *testing.T, pdAddr *net.UDPAddr, c *pdnet.Udp) {
	c.SendTo(pdAddr, &pdproto.RemStore{
		Seqno: proto.Int64(1),
		Id:    proto.Int32(3),
	})

	_, resp, _ := c.ReadFrom(make([]byte, 65536))
	assert.Equal(t, true, resp.(*pdproto.RemStoreResp).GetOk())

	c.SendTo(pdAddr, &pdproto.RemStore{
		Seqno: proto.Int64(1),
		Id:    proto.Int32(3),
	})

	_, resp, _ = c.ReadFrom(make([]byte, 65536))
	assert.Equal(t, true, resp.(*pdproto.RemStoreResp).GetOk())

	c.SendTo(pdAddr, &pdproto.RemStore{
		Seqno: proto.Int64(1),
		Id:    proto.Int32(1),
	})

	_, resp, _ = c.ReadFrom(make([]byte, 65536))
	assert.Equal(t, false, resp.(*pdproto.RemStoreResp).GetOk())
	fmt.Println(resp.(*pdproto.RemStoreResp).GetReason())

}

func testKvnodeAddStore(t *testing.T, pdAddr *net.UDPAddr, c *pdnet.Udp) {
	node1 := &testKvnode{
		nodeId:   1,
		isLeader: 1,
	}
	node1.udp, _ = pdnet.NewUdp("localhost:9110")

	node3 := &testKvnode{
		nodeId:   3,
		isLeader: 0,
	}
	node3.udp, _ = pdnet.NewUdp("localhost:9112")

	c.SendTo(pdAddr, &pdproto.AddKvnode{
		Seqno:       proto.Int64(1),
		NodeId:      proto.Int32(3),
		Service:     proto.String("localhost:8112"),
		RaftService: proto.String("http://127.0.0.1:22372"),
		UdpService:  proto.String("localhost:9112"),
	})

	_, resp, _ := c.ReadFrom(make([]byte, 65536))
	assert.Equal(t, true, resp.(*pdproto.AddKvnodeResp).GetOk())

	go node1.run()
	go node3.run()

	c.SendTo(pdAddr, &pdproto.KvnodeAddStore{
		Seqno:   proto.Int64(1),
		NodeId:  proto.Int32(3),
		StoreId: proto.Int32(1),
	})

	_, resp, _ = c.ReadFrom(make([]byte, 65536))
	assert.Equal(t, true, resp.(*pdproto.KvnodeAddStoreResp).GetOk())

	time.Sleep(time.Second)

	node1.udp.Close()
	node3.udp.Close()

}

func testKvnodeRemStore(t *testing.T, pdAddr *net.UDPAddr, c *pdnet.Udp) {
	node1 := &testKvnode{
		nodeId:   1,
		isLeader: 1,
	}
	node1.udp, _ = pdnet.NewUdp("localhost:9110")

	go node1.run()

	c.SendTo(pdAddr, &pdproto.KvnodeRemStore{
		Seqno:   proto.Int64(1),
		NodeId:  proto.Int32(3),
		StoreId: proto.Int32(1),
	})

	_, resp, _ := c.ReadFrom(make([]byte, 65536))
	assert.Equal(t, true, resp.(*pdproto.KvnodeRemStoreResp).GetOk())

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
			assert.NotNil(t, pd.slot2store[i])
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

	//先删除所有kv文件
	os.RemoveAll("./log/kv-1-1")
	os.RemoveAll("./log/kv-1-1-snap")

	pd, err := NewPd("localhost:8110", 1, "1@http://127.0.0.1:22378")
	if nil != err {
		panic(err)
	}

	waitCondition(func() bool {
		return pd.isLeader()
	})

	time.Sleep(time.Second * 5)

	pdAddr, _ := net.ResolveUDPAddr("udp", "localhost:8110")

	c, _ := pdnet.NewUdp("localhost:10010")

	c.SendTo(pdAddr, &pdproto.KvnodeBoot{
		NodeID: proto.Int32(1),
	})

	_, resp, err := c.ReadFrom(make([]byte, 65536))
	assert.Equal(t, "localhost:8110", resp.(*pdproto.KvnodeBootResp).GetService())
	assert.Equal(t, int32(1), resp.(*pdproto.KvnodeBootResp).GetStores()[0].GetId())

	c.SendTo(pdAddr, &pdproto.KvnodeBoot{
		NodeID: proto.Int32(2),
	})

	_, resp, err = c.ReadFrom(make([]byte, 65536))
	assert.Equal(t, "localhost:8111", resp.(*pdproto.KvnodeBootResp).GetService())
	assert.Equal(t, int32(2), resp.(*pdproto.KvnodeBootResp).GetStores()[0].GetId())

	c.SendTo(pdAddr, &pdproto.KvnodeBoot{
		NodeID: proto.Int32(10),
	})

	_, resp, err = c.ReadFrom(make([]byte, 65536))
	assert.Equal(t, false, resp.(*pdproto.KvnodeBootResp).GetOk())

	testAddNode(t, pdAddr, c)

	testRemNode(t, pdAddr, c)

	testAddStore(t, pdAddr, c)

	testRemStore(t, pdAddr, c)

	testKvnodeAddStore(t, pdAddr, c)

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
			assert.NotNil(t, pd.slot2store[i])
		}
		close(wait)
	})
	<-wait

	testSnapshot(t, pd)

	pd.Stop()

}
