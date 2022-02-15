//go test -coverpkg=github.com/sniperHW/flyfish/server/flygate,github.com/sniperHW/flyfish/server/flypd,github.com/sniperHW/flyfish/server/flykv -covermode=count -v -coverprofile=coverage.out -run=.
//go tool cover -html=coverage.out

package integrationTest

import (
	"fmt"
	"github.com/BurntSushi/toml"
	"github.com/sniperHW/flyfish/client"
	"github.com/sniperHW/flyfish/logger"
	"github.com/sniperHW/flyfish/pkg/bitmap"
	"github.com/sniperHW/flyfish/pkg/etcd/pkg/idutil"
	fnet "github.com/sniperHW/flyfish/pkg/net"
	flygate "github.com/sniperHW/flyfish/server/flygate"
	flykv "github.com/sniperHW/flyfish/server/flykv"
	flypd "github.com/sniperHW/flyfish/server/flypd"
	console "github.com/sniperHW/flyfish/server/flypd/console/http"
	snet "github.com/sniperHW/flyfish/server/net"
	sproto "github.com/sniperHW/flyfish/server/proto"
	sslot "github.com/sniperHW/flyfish/server/slot"
	"github.com/stretchr/testify/assert"
	"net"
	"os"
	"strings"
	"sync"
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

SnapshotCount             = 100
SnapshotCatchUpEntriesN   = 100

MainQueueMaxSize        = 10000

LruCheckInterval        = 1000              #每隔100ms执行一次lru剔除操作

MaxCachePerStore        = 100               #每组最大key数量，超过数量将会触发key剔除

SqlLoadPipeLineSize     = 200                  #sql加载管道线大小

SqlLoadQueueSize        = 10000                #sql加载请求队列大小，此队列每CacheGroup一个

SqlLoaderCount          = 5
SqlUpdaterCount         = 5

ProposalFlushInterval   = 100
ReadFlushInterval       = 10 

RaftLogDir              = "testRaftLog/flykv"

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
	DBType                  = "pgsql"
	[DBConfig]
		Host          = "localhost"
		Port          = 5432
		User	      = "sniper"
		Password      = "123456"
		DB            = "test"	
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

func newPD(t *testing.T, deploymentPath ...string) StopAble {
	conf, _ := flypd.LoadConfigStr(pdConfigStr)

	if len(deploymentPath) != 0 {
		conf.InitDepoymentPath = deploymentPath[0]
	}

	raftID := idutil.NewGenerator(1, time.Now()).Next()

	raftCluster := fmt.Sprintf("1@%d@http://localhost:18110@localhost:8110@voter", raftID)

	pd, _ := flypd.NewPd(1, 1, false, conf, raftCluster)
	return pd
}

func testkv(t *testing.T, c *client.Client) {
	fields := map[string]interface{}{}
	fields["age"] = 12
	fields["name"] = "sniperHW"

	fmt.Println("-----------------------------0----------------------------------")

	{

		fields := map[string]interface{}{}
		fields["age"] = 12
		fields["name"] = strings.Repeat("a", 4096)

		c.Set("users1", "sniperHWLargeName", fields).Exec()

		r := c.GetAll("users1", "sniperHWLargeName").Exec()
		assert.Nil(t, r.ErrCode)

		assert.Equal(t, r.Fields["name"].GetString(), fields["name"])
	}

	fmt.Println("-----------------------------1----------------------------------")

	{
		var wait sync.WaitGroup
		wait.Add(10)
		for i := 0; i < 10; i++ {
			c.Set("users1", "sniperHW", fields).AsyncExec(func(r *client.StatusResult) {
				assert.Nil(t, r.ErrCode)
				fmt.Println("version-----------", r.Version)
				wait.Done()
			})
		}

		c.GetAll("users1", "sniperHW").AsyncExec(func(r *client.SliceResult) {
			assert.Nil(t, r.ErrCode)
		})

		wait.Wait()
	}
	fmt.Println("-----------------------------get----------------------------------")
	{
		r := c.GetAll("users1", "sniperHW").Exec()
		assert.Nil(t, r.ErrCode)

		r = c.GetAllWithVersion("users1", "sniperHW", r.Version).Exec()
		assert.Equal(t, r.ErrCode, flykv.Err_record_unchange)

		r1 := c.Del("users1", "sniperHW", 1).Exec()
		assert.Equal(t, r1.ErrCode, flykv.Err_version_mismatch)

		r1 = c.Del("users1", "sniperHW", r.Version).Exec()
		assert.Nil(t, r1.ErrCode)

		r = c.GetAll("users1", "sniperHW").Exec()
		assert.Equal(t, r.ErrCode, flykv.Err_record_notexist)

		time.Sleep(time.Second)

		for {
			if nil == c.Kick("users1", "sniperHW").Exec().ErrCode {
				break
			}
			time.Sleep(time.Second)
		}

		r3 := c.Del("users1", "sniperHW").Exec()
		assert.Equal(t, r3.ErrCode, flykv.Err_record_notexist)

		r3 = c.Del("users1", "sniperHW").Exec()
		assert.Equal(t, r3.ErrCode, flykv.Err_record_notexist)

	}

	fmt.Println("-----------------------------set----------------------------------")

	{
		r1 := c.Set("users1", "sniperHW", fields).Exec()
		assert.Nil(t, r1.ErrCode)

		time.Sleep(time.Second)

		for {
			if nil == c.Kick("users1", "sniperHW").Exec().ErrCode {
				break
			}
			time.Sleep(time.Second)
		}

		r1 = c.Set("users1", "sniperHW", fields).Exec()
		assert.Nil(t, r1.ErrCode)

		time.Sleep(time.Second)

		for {
			if nil == c.Kick("users1", "sniperHW").Exec().ErrCode {
				break
			}
			time.Sleep(time.Second)
		}

		r1 = c.Set("users1", "sniperHW", fields, 1).Exec()
		assert.Equal(t, r1.ErrCode, flykv.Err_version_mismatch)

		r2 := c.Del("users1", "sniperHW").Exec()
		assert.Nil(t, r2.ErrCode)

		time.Sleep(time.Second)

		for {
			if nil == c.Kick("users1", "sniperHW").Exec().ErrCode {
				break
			}
			time.Sleep(time.Second)
		}

		r1 = c.Set("users1", "sniperHW", fields).Exec()
		assert.Nil(t, r1.ErrCode)

	}

	fmt.Println("-----------------------------setNx----------------------------------")

	{
		r1 := c.SetNx("users1", "sniperHW", fields).Exec()
		assert.Equal(t, r1.ErrCode, flykv.Err_record_exist)

		r2 := c.Del("users1", "sniperHW").Exec()
		assert.Nil(t, r2.ErrCode)

		r1 = c.SetNx("users1", "sniperHW", fields).Exec()
		assert.Nil(t, r1.ErrCode)

		time.Sleep(time.Second)

		for {
			if nil == c.Kick("users1", "sniperHW").Exec().ErrCode {
				break
			}
			time.Sleep(time.Second)
		}

		r1 = c.SetNx("users1", "sniperHW", fields).Exec()
		assert.Equal(t, r1.ErrCode, flykv.Err_record_exist)

		r2 = c.Del("users1", "sniperHW").Exec()
		assert.Nil(t, r2.ErrCode)

		time.Sleep(time.Second)

		for {
			if nil == c.Kick("users1", "sniperHW").Exec().ErrCode {
				break
			}
			time.Sleep(time.Second)
		}

		r1 = c.SetNx("users1", "sniperHW", fields).Exec()
		assert.Nil(t, r1.ErrCode)

	}

	fmt.Println("-----------------------------compareAndSet----------------------------------")
	{
		r1 := c.Get("users1", "sniperHW", "age").Exec()
		assert.Nil(t, r1.ErrCode)

		r2 := c.CompareAndSet("users1", "sniperHW", "age", r1.Fields["age"].GetValue(), 10).Exec()
		assert.Nil(t, r2.ErrCode)

		r2 = c.CompareAndSet("users1", "sniperHW", "age", 11, 10).Exec()
		assert.Equal(t, r2.ErrCode, flykv.Err_cas_not_equal)

		r3 := c.Del("users1", "sniperHW").Exec()
		assert.Nil(t, r3.ErrCode)

		r2 = c.CompareAndSet("users1", "sniperHW", "age", 11, 10).Exec()
		assert.Equal(t, r2.ErrCode, flykv.Err_record_notexist)

		time.Sleep(time.Second)

		for {
			if nil == c.Kick("users1", "sniperHW").Exec().ErrCode {
				break
			}
			time.Sleep(time.Second)
		}

		r2 = c.CompareAndSet("users1", "sniperHW", "age", 11, 10).Exec()
		assert.Equal(t, r2.ErrCode, flykv.Err_record_notexist)

		time.Sleep(time.Second)

		for {
			if nil == c.Kick("users1", "sniperHW").Exec().ErrCode {
				break
			}
			time.Sleep(time.Second)
		}

		r2 = c.CompareAndSet("users1", "sniperHW", "age", 10, 11).Exec()
		assert.Equal(t, r2.ErrCode, flykv.Err_record_notexist)

		r4 := c.Set("users1", "sniperHW", fields).Exec()
		assert.Nil(t, r4.ErrCode)

		time.Sleep(time.Second)

		for {
			if nil == c.Kick("users1", "sniperHW").Exec().ErrCode {
				break
			}
			time.Sleep(time.Second)
		}

		r2 = c.CompareAndSet("users1", "sniperHW", "age", 12, 11).Exec()
		assert.Nil(t, r2.ErrCode)

		time.Sleep(time.Second)

		for {
			if nil == c.Kick("users1", "sniperHW").Exec().ErrCode {
				break
			}
			time.Sleep(time.Second)
		}

		r2 = c.CompareAndSet("users1", "sniperHW", "age", 12, 11, 1).Exec()
		assert.Equal(t, r2.ErrCode, flykv.Err_version_mismatch)

	}

	fmt.Println("-----------------------------compareAndSetNx----------------------------------")

	{
		r2 := c.CompareAndSetNx("users1", "sniperHW", "age", 1, 10).Exec()
		assert.Equal(t, r2.ErrCode, flykv.Err_cas_not_equal)

		r2 = c.CompareAndSetNx("users1", "sniperHW", "age", 11, 12).Exec()
		assert.Nil(t, r2.ErrCode)

		r3 := c.Del("users1", "sniperHW").Exec()
		assert.Nil(t, r3.ErrCode)

		r2 = c.CompareAndSetNx("users1", "sniperHW", "age", 11, 12).Exec()
		assert.Nil(t, r2.ErrCode)

		time.Sleep(time.Second)

		for {
			if nil == c.Kick("users1", "sniperHW").Exec().ErrCode {
				break
			}
			time.Sleep(time.Second)
		}

		r2 = c.CompareAndSetNx("users1", "sniperHW", "age", 11, 12, 1).Exec()
		assert.Equal(t, r2.ErrCode, flykv.Err_version_mismatch)

		r3 = c.Del("users1", "sniperHW").Exec()
		assert.Nil(t, r3.ErrCode)

		time.Sleep(time.Second)

		for {
			if nil == c.Kick("users1", "sniperHW").Exec().ErrCode {
				break
			}
			time.Sleep(time.Second)
		}

		r2 = c.CompareAndSetNx("users1", "sniperHW", "age", 11, 12).Exec()
		assert.Nil(t, r2.ErrCode)

		time.Sleep(time.Second)

		for {
			if nil == c.Kick("users1", "sniperHW").Exec().ErrCode {
				break
			}
			time.Sleep(time.Second)
		}

		r2 = c.CompareAndSetNx("users1", "sniperHW", "age", 12, 11).Exec()
		assert.Nil(t, r2.ErrCode)

	}

	fmt.Println("-----------------------------incr----------------------------------")

	{
		r1 := c.IncrBy("users1", "sniperHW", "age", 2).Exec()
		assert.Nil(t, r1.ErrCode)
		assert.Equal(t, r1.Fields["age"].GetInt(), int64(13))

		r2 := c.Del("users1", "sniperHW").Exec()
		assert.Nil(t, r2.ErrCode)

		r1 = c.IncrBy("users1", "sniperHW", "age", 2).Exec()
		assert.Nil(t, r1.ErrCode)
		assert.Equal(t, r1.Fields["age"].GetInt(), int64(2))

		time.Sleep(time.Second)

		for {
			if nil == c.Kick("users1", "sniperHW").Exec().ErrCode {
				break
			}
			time.Sleep(time.Second)
		}

		r1 = c.IncrBy("users1", "sniperHW", "age", 2, 1).Exec()
		assert.Equal(t, r1.ErrCode, flykv.Err_version_mismatch)

		r2 = c.Del("users1", "sniperHW").Exec()
		assert.Nil(t, r2.ErrCode)

		time.Sleep(time.Second)
		for {
			if nil == c.Kick("users1", "sniperHW").Exec().ErrCode {
				break
			}
			time.Sleep(time.Second)
		}

		r1 = c.IncrBy("users1", "sniperHW", "age", 2).Exec()
		assert.Nil(t, r1.ErrCode)
		assert.Equal(t, r1.Fields["age"].GetInt(), int64(2))

		time.Sleep(time.Second)
		for {
			if nil == c.Kick("users1", "sniperHW").Exec().ErrCode {
				break
			}
			time.Sleep(time.Second)
		}

		r1 = c.IncrBy("users1", "sniperHW", "age", 2).Exec()
		assert.Nil(t, r1.ErrCode)
		assert.Equal(t, r1.Fields["age"].GetInt(), int64(4))

	}

	fmt.Println("-----------------------------decr----------------------------------")

	{

		r := c.Set("users1", "sniperHW", fields).Exec()
		assert.Nil(t, r.ErrCode)

		r1 := c.DecrBy("users1", "sniperHW", "age", 2).Exec()
		assert.Nil(t, r1.ErrCode)
		assert.Equal(t, r1.Fields["age"].GetInt(), int64(10))

		r2 := c.Del("users1", "sniperHW").Exec()
		assert.Nil(t, r2.ErrCode)

		r1 = c.DecrBy("users1", "sniperHW", "age", 2).Exec()
		assert.Nil(t, r1.ErrCode)
		assert.Equal(t, r1.Fields["age"].GetInt(), int64(-2))

		time.Sleep(time.Second)

		for {
			if nil == c.Kick("users1", "sniperHW").Exec().ErrCode {
				break
			}
			time.Sleep(time.Second)
		}

		r1 = c.DecrBy("users1", "sniperHW", "age", 2, 1).Exec()
		assert.Equal(t, r1.ErrCode, flykv.Err_version_mismatch)

		r2 = c.Del("users1", "sniperHW").Exec()
		assert.Nil(t, r2.ErrCode)

		time.Sleep(time.Second)
		for {
			if nil == c.Kick("users1", "sniperHW").Exec().ErrCode {
				break
			}
			time.Sleep(time.Second)
		}

		r1 = c.DecrBy("users1", "sniperHW", "age", 2).Exec()
		assert.Nil(t, r1.ErrCode)
		assert.Equal(t, r1.Fields["age"].GetInt(), int64(-2))

		time.Sleep(time.Second)
		for {
			if nil == c.Kick("users1", "sniperHW").Exec().ErrCode {
				break
			}
			time.Sleep(time.Second)
		}

		r1 = c.DecrBy("users1", "sniperHW", "age", 2).Exec()
		assert.Nil(t, r1.ErrCode)
		assert.Equal(t, r1.Fields["age"].GetInt(), int64(-4))

		time.Sleep(time.Second)
		for {
			if nil == c.Kick("users1", "sniperHW").Exec().ErrCode {
				break
			}
			time.Sleep(time.Second)
		}

		fmt.Println("---------------------------------")

		time.Sleep(time.Second)
		for {
			if nil == c.Kick("users1", "sniperHW").Exec().ErrCode {
				break
			}
			time.Sleep(time.Second)
		}
	}
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

		gate := client.QueryGate([]*net.UDPAddr{addr}, time.Second)
		if len(gate) > 0 {
			break
		}
		time.Sleep(time.Second)
	}

	fmt.Println("run client")

	c, _ := client.OpenClient(client.ClientConf{PD: []string{"localhost:8110"}})

	for i := 0; i < 100; i++ {
		fields := map[string]interface{}{}
		fields["age"] = 12
		name := fmt.Sprintf("sniperHW:%d", i)
		fields["name"] = name
		fields["phone"] = "123456789123456789123456789"
		for {
			r := c.Set("users10", name, fields).Exec()
			if nil == r.ErrCode {
				break
			} else {
				fmt.Println(name, r.ErrCode)
			}
		}
	}

	testkv(t, c)

	consoleClient := console.NewClient("localhost:8110")

	//添加一个kvnode
	resp, err := consoleClient.Call(&sproto.AddNode{
		SetID:       1,
		NodeID:      3,
		Host:        "localhost",
		ServicePort: 9320,
		RaftPort:    9321,
	}, &sproto.AddNodeResp{})

	fmt.Println(resp, err)

	//获取路由信息
	for {
		time.Sleep(time.Second)
		resp, err = consoleClient.Call(&sproto.QueryRouteInfo{}, &sproto.QueryRouteInfoResp{})
		sets := resp.(*sproto.QueryRouteInfoResp).Sets
		var set1 *sproto.RouteInfoSet
		for _, v := range sets {
			if v.SetID == 1 {
				set1 = v
			}
		}

		if nil != set1 && len(set1.Kvnodes) == 2 {
			break
		}
	}

	node1.Stop()

	node2.Stop()

	//start again
	node1, err = flykv.NewKvNode(1, false, kvConf, flykv.NewSqlDB())

	if nil != err {
		panic(err)
	}

	node2, err = flykv.NewKvNode(2, false, kvConf, flykv.NewSqlDB())

	if nil != err {
		panic(err)
	}

	for i := 0; i < 100; i++ {
		fields := map[string]interface{}{}
		fields["age"] = 12
		name := fmt.Sprintf("sniperHW:%d", i)
		fields["name"] = name
		fields["phone"] = "123456789123456789123456789"
		for {
			r := c.Set("users1", name, fields).Exec()
			if nil == r.ErrCode {
				break
			} else {
				fmt.Println(name, r.ErrCode)
			}
		}
	}

	node1.Stop()

	node2.Stop()

	fmt.Println("Stop gate")

	gate1.Stop()

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
		time.Sleep(time.Second)
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
		time.Sleep(time.Second)
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
		time.Sleep(time.Second)

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
		time.Sleep(time.Second)
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
		time.Sleep(time.Second)
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
		time.Sleep(time.Second)
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
		time.Sleep(time.Second)
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
		time.Sleep(time.Second)
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
		time.Sleep(time.Second)
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
		time.Sleep(time.Second)
	}

	pd.Stop()
	node1.Stop()
	node2.Stop()
	node3.Stop()

}

func TestStoreBalance(t *testing.T) {
	sslot.SlotCount = 128
	flypd.MinReplicaPerSet = 1
	flypd.StorePerSet = 3
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

	pd := newPD(t, "./deployment2.json")

	node1, err := flykv.NewKvNode(1, false, kvConf, flykv.NewSqlDB())

	if nil != err {
		panic(err)
	}

	//增加node2

	addr, _ := net.ResolveUDPAddr("udp", "localhost:8110")

	for {
		resp := snet.UdpCall([]*net.UDPAddr{addr},
			snet.MakeMessage(0, &sproto.AddNode{
				SetID:       1,
				NodeID:      2,
				Host:        "localhost",
				ServicePort: 9220,
				RaftPort:    9221,
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
		time.Sleep(time.Second)
	}

	//add learnstore

	for {
		resp := snet.UdpCall([]*net.UDPAddr{addr},
			snet.MakeMessage(0, &sproto.AddLearnerStoreToNode{
				SetID:  1,
				NodeID: 2,
				Store:  2,
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
		time.Sleep(time.Second)
	}

	logger.GetSugar().Infof("--------------------add learner 2:2 OK------------------------")

	node2, err := flykv.NewKvNode(2, true, kvConf, flykv.NewSqlDB())

	if nil != err {
		panic(err)
	}

	//promote to voter
	for {
		resp := snet.UdpCall([]*net.UDPAddr{addr},
			snet.MakeMessage(0, &sproto.PromoteLearnerStore{
				SetID:  1,
				NodeID: 2,
				Store:  2,
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
		time.Sleep(time.Second)
	}

	//增加node3
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
		time.Sleep(time.Second)
	}

	//add learnstore

	for {
		resp := snet.UdpCall([]*net.UDPAddr{addr},
			snet.MakeMessage(0, &sproto.AddLearnerStoreToNode{
				SetID:  1,
				NodeID: 3,
				Store:  3,
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
		time.Sleep(time.Second)
	}

	logger.GetSugar().Infof("--------------------add learner 3:3 OK------------------------")

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
				Store:  3,
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
		time.Sleep(time.Second)
	}

	/////////////////

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

		if nil != resp {
			ret := resp.(*sproto.GetSetStatusResp)
			if len(ret.Sets) > 0 {
				set1 := ret.Sets[0]
				nodeLeaderCount := make([]int, 3)
				totalLeaderCount := 0
				for k, v := range set1.Nodes {
					for _, vv := range v.Stores {
						if vv.IsLeader {
							nodeLeaderCount[k]++
							totalLeaderCount++
						}
					}
				}

				if totalLeaderCount == 3 {
					ok := true
					for _, v := range nodeLeaderCount {
						if v > 1 {
							ok = false
							break
						}
					}

					if ok {
						break
					}
				}
			}
		}
		time.Sleep(time.Second)
	}

	node1.Stop()
	node2.Stop()
	node3.Stop()
	pd.Stop()

}

func TestScan(t *testing.T) {
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

	node1, err := flykv.NewKvNode(1, false, kvConf, flykv.NewSqlDB())

	if nil != err {
		panic(err)
	}

	node2, err := flykv.NewKvNode(2, false, kvConf, flykv.NewSqlDB())

	if nil != err {
		panic(err)
	}

	gateConf, _ := flygate.LoadConfigStr(flyGateConfigStr)

	gate1, err := flygate.NewFlyGate(gateConf, "localhost:10110")

	if nil != err {
		panic(err)
	}

	for {

		addr, _ := net.ResolveUDPAddr("udp", "localhost:8110")

		gate := client.QueryGate([]*net.UDPAddr{addr}, time.Second)
		if len(gate) > 0 {
			break
		}
		time.Sleep(time.Second)
	}

	fmt.Println("run client")

	c, _ := client.OpenClient(client.ClientConf{PD: []string{"localhost:8110"}})

	for i := 0; i < 100; i++ {
		fields := map[string]interface{}{}
		fields["age"] = i
		name := fmt.Sprintf("sniperHW:%d", i)
		fields["name"] = name
		fields["phone"] = "123456789123456789123456789"
		r := c.Set("users1", name, fields).Exec()
		assert.Nil(t, r.ErrCode)
	}

	fmt.Println("set ok")

	sc, _ := client.NewScanner(client.ClientConf{PD: []string{"localhost:8110"}}, "users1", []string{"name", "age", "phone"})

	count := 0

	for {
		row, err := sc.Next(time.Now().Add(time.Second * 10))

		if nil != err {
			panic(err)
		}

		if nil != row {
			fmt.Println(row.Key)
			count++
		} else {
			break
		}
	}

	fmt.Println("count", count)

	node1.Stop()
	node2.Stop()

	os.RemoveAll("./testRaftLog/flykv")

	node1, err = flykv.NewKvNode(1, false, kvConf, flykv.NewSqlDB())

	if nil != err {
		panic(err)
	}

	node2, err = flykv.NewKvNode(2, false, kvConf, flykv.NewSqlDB())

	if nil != err {
		panic(err)
	}

	fmt.Println("again")

	sc, _ = client.NewScanner(client.ClientConf{PD: []string{"localhost:8110"}}, "users1", []string{"name", "age", "phone"})

	count = 0

	for {
		row, err := sc.Next(time.Now().Add(time.Second * 10))

		if nil != err {
			panic(err)
		}

		if nil != row {
			fmt.Println(row.Key)
			count++
		} else {
			break
		}
	}

	fmt.Println("count", count)

	node1.Stop()
	node2.Stop()
	gate1.Stop()
	pd.Stop()

}
