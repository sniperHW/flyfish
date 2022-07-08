package flypd

//go test -race -covermode=atomic -v -coverprofile=coverage.out -run=.
//go tool cover -html=coverage.out

import (
	"fmt"
	flyfishclient "github.com/sniperHW/flyfish/client"
	"github.com/sniperHW/flyfish/db"
	"github.com/sniperHW/flyfish/db/sql"
	"github.com/sniperHW/flyfish/logger"
	"github.com/sniperHW/flyfish/pkg/etcd/pkg/idutil"
	fnet "github.com/sniperHW/flyfish/pkg/net"
	consoleHttp "github.com/sniperHW/flyfish/server/flypd/console/http"
	consoleUdp "github.com/sniperHW/flyfish/server/flypd/console/udp"
	snet "github.com/sniperHW/flyfish/server/net"
	sproto "github.com/sniperHW/flyfish/server/proto"
	sslot "github.com/sniperHW/flyfish/server/slot"
	"github.com/stretchr/testify/assert"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"sync"
	"testing"
	"time"
)

var RaftIDGen *idutil.Generator = idutil.NewGenerator(1, time.Now())

func init() {
	sslot.SlotCount = 128
	go func() {
		http.ListenAndServe("localhost:19111", nil)
	}()
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
				//GetSugar().Infof("on NotifyNodeStoreOp %v", msg.(*sproto.NotifyNodeStoreOp))
				n.udp.SendTo(from, snet.MakeMessage(context, &sproto.NodeStoreOpOk{}))
			case *sproto.NotifySlotTransIn:
				notify := msg.(*sproto.NotifySlotTransIn)
				//fmt.Println("on slot trans in", notify.Slot, n.nodeId)
				n.udp.SendTo(from, snet.MakeMessage(context, &sproto.SlotTransInOk{
					Slot: notify.Slot,
				}))
			case *sproto.NotifySlotTransOut:
				notify := msg.(*sproto.NotifySlotTransOut)
				//fmt.Println("on slot trans out", notify.Slot, n.nodeId)
				n.udp.SendTo(from, snet.MakeMessage(context, &sproto.SlotTransOutOk{
					Slot: notify.Slot,
				}))
			case *sproto.NotifyUpdateMeta:
				notify := msg.(*sproto.NotifyUpdateMeta)
				n.metaVersion = notify.Version
			case *sproto.IsTransInReady:
				//fmt.Println("on IsTransInReady")
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

var configStr string = `

	MainQueueMaxSize = 1000
	RaftLogDir       = "raftLog"
	RaftLogPrefix    = "pd"
	InitMetaPath = "./test/initmeta.json"
	InitDepoymentPath="./test/deployment.json"


	[DBConfig]
		DBType        = "pgsql"
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

var conf *Config

func init() {
	l := logger.NewZapLogger("testPd.log", "./log", "Debug", 100, 14, 10, true)
	InitLogger(l)

	conf, _ = LoadConfigStr(configStr)

	//先删除table1
	dbc, _ := sql.SqlOpen(conf.DBConfig.DBType, conf.DBConfig.Host, conf.DBConfig.Port, conf.DBConfig.DB, conf.DBConfig.User, conf.DBConfig.Password)

	sql.DropTable(dbc, &db.TableDef{
		Name:      "table1",
		DbVersion: 0,
	})

	sql.DropTable(dbc, &db.TableDef{
		Name:      "table2",
		DbVersion: 1,
	})

	sql.DropTable(dbc, &db.TableDef{
		Name:      "table2",
		DbVersion: 3,
	})
}

func TestAddRemoveTable(t *testing.T) {
	os.RemoveAll("./raftLog")

	raftID := RaftIDGen.Next()

	pd, _ := NewPd(1, 1, false, conf, fmt.Sprintf("1@%d@http://localhost:18110@localhost:8110@", raftID))

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

		for {
			if resp, _ := consoleUdp.Call([]string{"localhost:8110"}, req, &sproto.MetaAddTableResp{}, time.Second); nil != resp {
				break
			}
			time.Sleep(time.Second)
		}
	}

	//remove table2
	{
		req := &sproto.MetaRemoveTable{
			Table:   "table2",
			Version: 2,
		}

		for {
			if resp, _ := consoleUdp.Call([]string{"localhost:8110"}, req, &sproto.MetaRemoveTableResp{}, time.Second); nil != resp {
				if resp.(*sproto.MetaRemoveTableResp).Ok {
					break
				}
			}
			time.Sleep(time.Second)
		}
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

		for {
			if resp, _ := consoleUdp.Call([]string{"localhost:8110"}, req, &sproto.MetaAddTableResp{}, time.Second); nil != resp {
				if resp.(*sproto.MetaAddTableResp).Ok {
					break
				}
			}
			time.Sleep(time.Second)
		}
	}

	pd.Stop()

	pd, _ = NewPd(1, 1, false, conf, fmt.Sprintf("1@%d@http://localhost:18110@localhost:8110@", raftID))

	for {
		if pd.isLeader() {
			break
		} else {
			time.Sleep(time.Second)
		}
	}

	pd.Stop()

}

func TestAddRemoveFields(t *testing.T) {

	os.RemoveAll("./raftLog")

	raftID := RaftIDGen.Next()

	pd, _ := NewPd(1, 1, false, conf, fmt.Sprintf("1@%d@http://localhost:18110@localhost:8110@", raftID))

	{
		//add field
		req := &sproto.MetaAddFields{
			Table:   "table1",
			Version: 1,
		}

		req.Fields = append(req.Fields, &sproto.MetaFiled{
			Name:    "field4",
			Type:    "string",
			Default: "hello",
		})

		for {
			if resp, _ := consoleUdp.Call([]string{"localhost:8110"}, req, &sproto.MetaAddFieldsResp{}, time.Second); nil != resp {
				if resp.(*sproto.MetaAddFieldsResp).Ok {
					break
				}
			}
			time.Sleep(time.Second)
		}
	}

	{
		//remove field
		req := &sproto.MetaRemoveFields{
			Table:   "table1",
			Fields:  []string{"field4"},
			Version: 2,
		}

		for {
			if resp, _ := consoleUdp.Call([]string{"localhost:8110"}, req, &sproto.MetaRemoveFieldsResp{}, time.Second); nil != resp {
				if resp.(*sproto.MetaRemoveFieldsResp).Ok {
					break
				}
			}
			time.Sleep(time.Second)
		}
	}

	{
		//add field again
		req := &sproto.MetaAddFields{
			Table:   "table1",
			Version: 3,
		}

		req.Fields = append(req.Fields, &sproto.MetaFiled{
			Name: "field4",
			Type: "int",
		})

		for {
			if resp, _ := consoleUdp.Call([]string{"localhost:8110"}, req, &sproto.MetaAddFieldsResp{}, time.Second); nil != resp {
				if resp.(*sproto.MetaAddFieldsResp).Ok {
					break
				} else {
					GetSugar().Errorf("%s", resp.(*sproto.MetaAddFieldsResp).Reason)
				}
			}
			time.Sleep(time.Second)
		}
	}

	pd.Stop()

	pd, _ = NewPd(1, 1, false, conf, fmt.Sprintf("1@%d@http://localhost:18110@localhost:8110@", raftID))

	for {
		if pd.isLeader() {
			break
		} else {
			time.Sleep(time.Second)
		}
	}

	pd.Stop()

}

func TestAddRemovePd(t *testing.T) {
	os.RemoveAll("./raftLog")

	var raftID2 uint64

	raftID := RaftIDGen.Next()

	pd1, _ := NewPd(1, 1, false, conf, fmt.Sprintf("1@%d@http://localhost:18110@localhost:8110@", raftID))

	var raftCluster string
	var cluster int

	{
		req := &sproto.AddPdNode{
			Id:        2,
			ClientUrl: "localhost:8111",
			Url:       "http://localhost:18111",
		}

		for {
			if resp, _ := consoleUdp.Call([]string{"localhost:8110"}, req, &sproto.AddPdNodeResp{}, time.Second); nil != resp {
				if resp.(*sproto.AddPdNodeResp).Ok {
					ret := resp.(*sproto.AddPdNodeResp)
					raftID2 = ret.RaftID
					raftCluster = ret.RaftCluster
					cluster = int(ret.Cluster)
					break
				}
			}
			time.Sleep(time.Second)
		}
	}

	pd2, _ := NewPd(2, cluster, true, conf, raftCluster)

	for {
		if resp, err := consoleUdp.Call([]string{"localhost:8110", "localhost:8111"}, &sproto.ListPdMembers{}, &sproto.ListPdMembersResp{}, time.Second); nil != resp {
			assert.Equal(t, 2, len(resp.(*sproto.ListPdMembersResp).Members))
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
			RaftID: raftID2,
		}, &sproto.RemovePdNodeResp{}, time.Second); nil != resp && resp.(*sproto.RemovePdNodeResp).Reason == "membership: ID not found" {
			break
		} else {
			GetSugar().Errorf("RemovePdNode err:%v %v", err, resp)
		}
		time.Sleep(time.Second)
	}

	pd1.Stop()

	pd2.Stop()

	pd1, _ = NewPd(1, 1, false, conf, fmt.Sprintf("1@%d@http://localhost:18110@localhost:8110@", raftID))

	for {
		if resp, err := consoleUdp.Call([]string{"localhost:8110", "localhost:8111"}, &sproto.ListPdMembers{}, &sproto.ListPdMembersResp{}, time.Second); nil != resp {
			assert.Equal(t, 1, len(resp.(*sproto.ListPdMembersResp).Members))
			GetSugar().Infof("%v", resp.(*sproto.ListPdMembersResp).Members)
			break
		} else {
			GetSugar().Errorf("ListPdMembers err:%v %v", err, resp)
		}
		time.Sleep(time.Second)
	}

	pd1.Stop()
}

func TestAddRemNode(t *testing.T) {
	os.RemoveAll("./raftLog")

	raftID := RaftIDGen.Next()

	pd, _ := NewPd(1, 1, false, conf, fmt.Sprintf("1@%d@http://localhost:18110@localhost:8110@", raftID))

	for {

		req := &sproto.AddNode{
			SetID:       1,
			NodeID:      2,
			Host:        "localhost",
			ServicePort: 9200,
			RaftPort:    9201,
		}

		if resp, _ := consoleUdp.Call([]string{"localhost:8110"}, req, &sproto.AddNodeResp{}, time.Second); nil != resp {
			if resp.(*sproto.AddNodeResp).Ok {
				break
			} else {
				GetSugar().Infof("%s", resp.(*sproto.AddNodeResp).Reason)
			}
		}
		time.Sleep(time.Second)
	}

	node1 := &testKvnode{
		nodeId: 1,
	}

	node1.udp, _ = fnet.NewUdp("localhost:9100", snet.Pack, snet.Unpack)

	go node1.run()

	node2 := &testKvnode{
		nodeId: 2,
	}

	node2.udp, _ = fnet.NewUdp("localhost:9200", snet.Pack, snet.Unpack)

	go node2.run()

	//等待所有store成为voter

	GetSugar().Infof("wait................")

	for {
		c := consoleHttp.NewClient("localhost:8110")
		resp, err := c.Call(&sproto.GetKvStatus{}, &sproto.GetKvStatusResp{})
		if nil != err {
			GetSugar().Errorf("%v", err)
		} else {
			ok := func() bool {
				s := resp.(*sproto.GetKvStatusResp).Sets[0]
				for _, v := range s.Nodes {
					if int(v.NodeID) == 2 {
						for _, vv := range v.Stores {

							if vv.StoreType != int32(VoterStore) {
								return false
							}
						}
						return len(v.Stores) == StorePerSet
					}
				}
				return false
			}()

			if ok {
				break
			} else {
				time.Sleep(time.Second)
			}
		}
	}

	GetSugar().Infof("here")

	for {

		req := &sproto.RemNode{
			SetID:  1,
			NodeID: 2,
		}

		if resp, _ := consoleUdp.Call([]string{"localhost:8110"}, req, &sproto.RemNodeResp{}, time.Second); nil != resp {
			if resp.(*sproto.RemNodeResp).Ok {
				break
			} else if resp.(*sproto.RemNodeResp).Reason == "node not found" {
				break
			}
		}
		time.Sleep(time.Second)
	}

	//等待node被成功移除

	for {
		c := consoleHttp.NewClient("localhost:8110")
		resp, err := c.Call(&sproto.GetKvStatus{}, &sproto.GetKvStatusResp{})
		if nil != err {
			GetSugar().Errorf("%v", err)
		} else {
			ok := func() bool {
				s := resp.(*sproto.GetKvStatusResp).Sets[0]
				for _, v := range s.Nodes {
					if int(v.NodeID) == 2 {
						return false
					}
				}
				return true
			}()

			if ok {
				break
			} else {
				time.Sleep(time.Second)
			}
		}
	}

	node2.stop()

	node1.stop()

	pd.Stop()

	pd, _ = NewPd(1, 1, false, conf, fmt.Sprintf("1@%d@http://localhost:18110@localhost:8110@", raftID))

	for {
		if pd.isLeader() {
			break
		} else {
			time.Sleep(time.Second)
		}
	}

	pd.Stop()

}

func TestAddRemSet(t *testing.T) {
	os.RemoveAll("./raftLog")

	raftID := RaftIDGen.Next()

	pd, _ := NewPd(1, 1, false, conf, fmt.Sprintf("1@%d@http://localhost:18110@localhost:8110@", raftID))

	for {

		req := &sproto.AddSet{
			&sproto.DeploymentSet{
				SetID: 2,
				Nodes: []*sproto.DeploymentKvnode{
					&sproto.DeploymentKvnode{
						NodeID:      2,
						Host:        "localhost",
						ServicePort: 9200,
						RaftPort:    9201,
					},
				},
			},
		}

		if resp, _ := consoleUdp.Call([]string{"localhost:8110"}, req, &sproto.AddSetResp{}, time.Second); nil != resp {
			if resp.(*sproto.AddSetResp).Ok {
				break
			} else if resp.(*sproto.AddSetResp).Reason == "set already exists" {
				break
			}
		}
		time.Sleep(time.Second)
	}

	expectCount := (sslot.SlotCount / 6) * 3

	waitCondition(func() bool {
		ret := make(chan bool, 1)
		pd.mainque.AppendHighestPriotiryItem(func() {
			if len(pd.SlotTransferMgr.Plan)+len(pd.SlotTransferMgr.Transactions) == expectCount {
				ret <- true
			} else {
				ret <- false
			}
		})
		return <-ret
	})

	pd.Stop()

	pd, _ = NewPd(1, 1, false, conf, fmt.Sprintf("1@%d@http://localhost:18110@localhost:8110@", raftID))

	for {
		if pd.isLeader() {
			break
		} else {
			time.Sleep(time.Second)
		}
	}

	waitCondition(func() bool {
		ret := make(chan bool, 1)
		pd.mainque.AppendHighestPriotiryItem(func() {
			if len(pd.SlotTransferMgr.Plan)+len(pd.SlotTransferMgr.Transactions) == expectCount {
				ret <- true
			} else {
				ret <- false
			}
		})
		return <-ret
	})

	pd.Stop()

	pd, _ = NewPd(1, 1, false, conf, fmt.Sprintf("1@%d@http://localhost:18110@localhost:8110@", raftID))

	for {

		req := &sproto.RemSet{
			SetID: 2,
		}

		if resp, _ := consoleUdp.Call([]string{"localhost:8110"}, req, &sproto.RemSetResp{}, time.Second); nil != resp {
			if resp.(*sproto.RemSetResp).Ok {
				break
			} else if resp.(*sproto.RemSetResp).Reason == "set not exists" {
				break
			}
		}
		time.Sleep(time.Second)
	}

	waitCondition(func() bool {
		ret := make(chan bool, 1)
		pd.mainque.AppendHighestPriotiryItem(func() {
			if len(pd.SlotTransferMgr.Plan)+len(pd.SlotTransferMgr.Transactions)+len(pd.SlotTransferMgr.FreeSlots) == 0 {
				ret <- true
			} else {
				ret <- false
			}
		})
		return <-ret
	})

	pd.Stop()

	pd, _ = NewPd(1, 1, false, conf, fmt.Sprintf("1@%d@http://localhost:18110@localhost:8110@", raftID))

	for {
		if pd.isLeader() {
			break
		} else {
			time.Sleep(time.Second)
		}
	}

	waitCondition(func() bool {
		ret := make(chan bool, 1)
		pd.mainque.AppendHighestPriotiryItem(func() {
			if len(pd.SlotTransferMgr.Plan)+len(pd.SlotTransferMgr.Transactions)+len(pd.SlotTransferMgr.FreeSlots) == 0 {
				ret <- true
			} else {
				ret <- false
			}
		})
		return <-ret
	})

	pd.Stop()

}

func TestSlotBlance(t *testing.T) {
	os.RemoveAll("./raftLog")

	raftID := RaftIDGen.Next()

	pd, _ := NewPd(1, 1, false, conf, fmt.Sprintf("1@%d@http://localhost:18110@localhost:8110@", raftID))

	node1 := &testKvnode{
		nodeId: 1,
	}

	node1.udp, _ = fnet.NewUdp("localhost:9100", snet.Pack, snet.Unpack)

	go node1.run()

	for {
		req := &sproto.AddSet{
			&sproto.DeploymentSet{
				SetID: 2,
				Nodes: []*sproto.DeploymentKvnode{
					&sproto.DeploymentKvnode{
						NodeID:      2,
						Host:        "localhost",
						ServicePort: 9200,
						RaftPort:    9201,
					},
				},
			},
		}

		if resp, _ := consoleUdp.Call([]string{"localhost:8110"}, req, &sproto.AddSetResp{}, time.Second); nil != resp {
			if resp.(*sproto.AddSetResp).Ok {
				break
			} else if resp.(*sproto.AddSetResp).Reason == "set already exists" {
				break
			}
		}
		time.Sleep(time.Second)
	}

	node2 := &testKvnode{
		nodeId: 2,
	}

	node2.udp, _ = fnet.NewUdp("localhost:9200", snet.Pack, snet.Unpack)

	go node2.run()

	time.Sleep(time.Millisecond * 10)

	//等待balance完成
	average := sslot.SlotCount / 6

	for {
		c := consoleHttp.NewClient("localhost:8110")
		resp, err := c.Call(&sproto.GetKvStatus{}, &sproto.GetKvStatusResp{})
		if nil != err {
			GetSugar().Errorf("%v", err)
		} else {
			totalSlot := 0
			ok := func() bool {
				for _, v := range resp.(*sproto.GetKvStatusResp).Sets {
					for _, vv := range v.Stores {
						slotCount := int(vv.Slotcount)
						if !(slotCount >= average && slotCount <= average+1) {
							return false
						} else {
							totalSlot += slotCount
						}
					}
				}
				return true
			}()
			if ok && totalSlot == sslot.SlotCount {
				break
			} else {
				time.Sleep(time.Second)
			}
		}
	}

	//将set2标记clear

	for {
		req := &sproto.SetMarkClear{
			SetID: 2,
		}

		if resp, _ := consoleUdp.Call([]string{"localhost:8110"}, req, &sproto.SetMarkClearResp{}, time.Second); nil != resp {
			if resp.(*sproto.SetMarkClearResp).Ok {
				break
			} else if resp.(*sproto.SetMarkClearResp).Reason == "already mark clear" {
				break
			}
		}
		time.Sleep(time.Second)
	}

	for {
		c := consoleHttp.NewClient("localhost:8110")
		resp, err := c.Call(&sproto.GetKvStatus{}, &sproto.GetKvStatusResp{})
		if nil != err {
			GetSugar().Errorf("%v", err)
		} else {
			ok := func() bool {
				for _, v := range resp.(*sproto.GetKvStatusResp).Sets {
					if v.SetID == int32(1) {
						total := 0
						for _, vv := range v.Stores {
							total += int(vv.Slotcount)
						}
						return total == sslot.SlotCount
					}
				}
				return false
			}()
			if ok {
				break
			} else {
				time.Sleep(time.Second)
			}
		}
	}

	node1.stop()
	node2.stop()

	pd.Stop()

	pd, _ = NewPd(1, 1, false, conf, fmt.Sprintf("1@%d@http://localhost:18110@localhost:8110@", raftID))

	for {
		if pd.isLeader() {
			break
		} else {
			time.Sleep(time.Second)
		}
	}

	pd.Stop()
}

func TestSequenceID(t *testing.T) {
	os.RemoveAll("./raftLog")

	raftID := RaftIDGen.Next()

	pd, _ := NewPd(1, 1, false, conf, fmt.Sprintf("1@%d@http://localhost:18110@localhost:8110@", raftID))

	var pdAddr []*net.UDPAddr

	if addr, err := net.ResolveUDPAddr("udp", "localhost:8110"); nil == err {
		pdAddr = append(pdAddr, addr)
	} else {
		panic(err)
	}

	{
		s := flyfishclient.NewSequence(pdAddr, 2000)
		m := map[int64]bool{}
		a := []int64{}
		for i := 0; i < 1999; i++ {
			next, _ := s.Next(time.Second * 6)
			m[next] = true
			a = append(a, next)
		}
		assert.Equal(t, len(m), 1999)
		assert.Equal(t, int64(1), a[0])
		assert.Equal(t, int64(1999), a[1998])
		s.Close()
	}

	{
		s := flyfishclient.NewSequence(pdAddr, 2000)
		next, _ := s.Next(time.Second * 6)
		//前面的测试会order 2次，因此现在从4001开始
		assert.Equal(t, next, int64(4001))
		begTime := time.Now()
		for i := 0; i < 1000000; i++ {
			_, _ = s.Next(time.Second * 6)
		}
		logger.GetSugar().Infof("use %v", time.Now().Sub(begTime))
		next, _ = s.Next(time.Second * 6)
		assert.Equal(t, next, int64(1004002))
		s.Close()
	}

	{
		var mu sync.Mutex
		var wait sync.WaitGroup
		m := map[int64]bool{}
		s := flyfishclient.NewSequence(pdAddr, 2000)
		begTime := time.Now()
		for i := 0; i < 4; i++ {
			wait.Add(1)
			go func() {
				for i := 0; i < 100000; i++ {
					next, _ := s.Next(time.Second * 6)
					mu.Lock()
					m[next] = true
					mu.Unlock()
				}
				wait.Done()
			}()
		}
		wait.Wait()
		logger.GetSugar().Infof("use %v", time.Now().Sub(begTime))
		//确保没有发生重复
		assert.Equal(t, len(m), 400000)
		s.Close()
	}

	pd.Stop()

	pd, _ = NewPd(1, 1, false, conf, fmt.Sprintf("1@%d@http://localhost:18110@localhost:8110@", raftID))

	for {
		if pd.isLeader() {
			break
		} else {
			time.Sleep(time.Second)
		}
	}

	pd.Stop()

}
