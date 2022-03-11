//go test -race -coverpkg=github.com/sniperHW/flyfish/server/flygate,github.com/sniperHW/flyfish/server/flypd,github.com/sniperHW/flyfish/server/flykv,github.com/sniperHW/flyfish/client -covermode=atomic -v -coverprofile=coverage.out -run=.
//go tool cover -html=coverage.out

package integrationTest

import (
	"fmt"
	"github.com/BurntSushi/toml"
	"github.com/sniperHW/flyfish/client"
	"github.com/sniperHW/flyfish/db"
	"github.com/sniperHW/flyfish/db/sql"
	"github.com/sniperHW/flyfish/errcode"
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
	"sync/atomic"
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

SnapshotCurrentCount    = 1

SnapshotCount             = 100
SnapshotCatchUpEntriesN   = 100

MainQueueMaxSize        = 10000

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

#WriteBackMode           = "WriteThrough"

[ClusterConfig]
PD                      = "localhost:8110"

[DBConfig]
DBType        = "%s"
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
	[DBConfig]
		DBType        = "pgsql"
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

func newPD(t *testing.T, raftID uint64, deploymentPath ...string) (StopAble, uint64) {
	conf, _ := flypd.LoadConfigStr(pdConfigStr)

	if len(deploymentPath) != 0 {
		conf.InitDepoymentPath = deploymentPath[0]
	}

	if 0 == raftID {
		raftID = idutil.NewGenerator(0, time.Now()).Next()
	}

	raftCluster := fmt.Sprintf("1@%d@http://localhost:18110@localhost:8110@voter", raftID)

	pd, _ := flypd.NewPd(1, 1, false, conf, raftCluster)

	return pd, raftID
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

		r1 := c.Del("users1", "sniperHW").Exec()
		assert.Nil(t, r1.ErrCode)

		r = c.GetAll("users1", "sniperHW").Exec()
		assert.Equal(t, r.ErrCode, flykv.Err_record_notexist)

		assert.Nil(t, c.Kick("users1", "sniperHW").Exec().ErrCode)

		r3 := c.Del("users1", "sniperHW").Exec()
		assert.Equal(t, r3.ErrCode, flykv.Err_record_notexist)

		r3 = c.Del("users1", "sniperHW").Exec()
		assert.Equal(t, r3.ErrCode, flykv.Err_record_notexist)

	}

	fmt.Println("-----------------------------set----------------------------------")

	{
		r1 := c.Set("users1", "sniperHW", fields).Exec()
		assert.Nil(t, r1.ErrCode)

		assert.Nil(t, c.Kick("users1", "sniperHW").Exec().ErrCode)

		r1 = c.Set("users1", "sniperHW", fields).Exec()
		assert.Nil(t, r1.ErrCode)

		assert.Nil(t, c.Kick("users1", "sniperHW").Exec().ErrCode)

		r1 = c.Set("users1", "sniperHW", fields, 1).Exec()
		assert.Equal(t, r1.ErrCode, flykv.Err_version_mismatch)

		r2 := c.Del("users1", "sniperHW").Exec()
		assert.Nil(t, r2.ErrCode)

		assert.Nil(t, c.Kick("users1", "sniperHW").Exec().ErrCode)

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

		assert.Nil(t, c.Kick("users1", "sniperHW").Exec().ErrCode)

		r1 = c.SetNx("users1", "sniperHW", fields).Exec()
		assert.Equal(t, r1.ErrCode, flykv.Err_record_exist)

		r2 = c.Del("users1", "sniperHW").Exec()
		assert.Nil(t, r2.ErrCode)

		assert.Nil(t, c.Kick("users1", "sniperHW").Exec().ErrCode)

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

		assert.Nil(t, c.Kick("users1", "sniperHW").Exec().ErrCode)

		r2 = c.CompareAndSet("users1", "sniperHW", "age", 11, 10).Exec()
		assert.Equal(t, r2.ErrCode, flykv.Err_record_notexist)

		assert.Nil(t, c.Kick("users1", "sniperHW").Exec().ErrCode)

		r2 = c.CompareAndSet("users1", "sniperHW", "age", 10, 11).Exec()
		assert.Equal(t, r2.ErrCode, flykv.Err_record_notexist)

		r4 := c.Set("users1", "sniperHW", fields).Exec()
		assert.Nil(t, r4.ErrCode)

		assert.Nil(t, c.Kick("users1", "sniperHW").Exec().ErrCode)

		r2 = c.CompareAndSet("users1", "sniperHW", "age", 12, 11).Exec()
		assert.Nil(t, r2.ErrCode)

		assert.Nil(t, c.Kick("users1", "sniperHW").Exec().ErrCode)

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

		assert.Nil(t, c.Kick("users1", "sniperHW").Exec().ErrCode)

		r3 = c.Del("users1", "sniperHW").Exec()
		assert.Nil(t, r3.ErrCode)

		assert.Nil(t, c.Kick("users1", "sniperHW").Exec().ErrCode)

		r2 = c.CompareAndSetNx("users1", "sniperHW", "age", 11, 12).Exec()
		assert.Nil(t, r2.ErrCode)

		assert.Nil(t, c.Kick("users1", "sniperHW").Exec().ErrCode)

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

		assert.Nil(t, c.Kick("users1", "sniperHW").Exec().ErrCode)

		r2 = c.Del("users1", "sniperHW").Exec()
		assert.Nil(t, r2.ErrCode)

		assert.Nil(t, c.Kick("users1", "sniperHW").Exec().ErrCode)

		r1 = c.IncrBy("users1", "sniperHW", "age", 2).Exec()
		assert.Nil(t, r1.ErrCode)
		assert.Equal(t, r1.Fields["age"].GetInt(), int64(2))

		assert.Nil(t, c.Kick("users1", "sniperHW").Exec().ErrCode)

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

		assert.Nil(t, c.Kick("users1", "sniperHW").Exec().ErrCode)

		r2 = c.Del("users1", "sniperHW").Exec()
		assert.Nil(t, r2.ErrCode)

		assert.Nil(t, c.Kick("users1", "sniperHW").Exec().ErrCode)

		r1 = c.DecrBy("users1", "sniperHW", "age", 2).Exec()
		assert.Nil(t, r1.ErrCode)
		assert.Equal(t, r1.Fields["age"].GetInt(), int64(-2))

		assert.Nil(t, c.Kick("users1", "sniperHW").Exec().ErrCode)

		r1 = c.DecrBy("users1", "sniperHW", "age", 2).Exec()
		assert.Nil(t, r1.ErrCode)
		assert.Equal(t, r1.Fields["age"].GetInt(), int64(-4))

		assert.Nil(t, c.Kick("users1", "sniperHW").Exec().ErrCode)

		fmt.Println("---------------------------------")

		assert.Nil(t, c.Kick("users1", "sniperHW").Exec().ErrCode)
	}

	fmt.Println("-----------------------------mget----------------------------------")

	{
		r := client.MGet(c.GetAll("users1", "sniperHW"), c.GetAll("users1", "sniperHW21"), c.GetAll("users1", "sniperHW2")).Exec()
		assert.Equal(t, 3, len(r))
	}
}

func TestFlygate(t *testing.T) {
	sslot.SlotCount = 128
	flypd.MinReplicaPerSet = 1
	flypd.StorePerSet = 2

	os.RemoveAll("./testRaftLog")

	pd, pdRaftID := newPD(t, 0)

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
		assert.Nil(t, c.Set("users1", name, fields).Exec().ErrCode)
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

	node3, err := flykv.NewKvNode(3, true, kvConf, flykv.NewSqlDB())

	if nil != err {
		panic(err)
	}

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

	resp, err = consoleClient.Call(&sproto.GetSetStatus{}, &sproto.GetSetStatusResp{})
	if nil == err {
		kvcount := 0
		for _, set := range resp.(*sproto.GetSetStatusResp).Sets {
			kvcount += int(set.Kvcount)
		}

		assert.Equal(t, true, kvcount > 0)

	} else {
		fmt.Println(err)
	}

	//向node3添加store1

	resp, err = consoleClient.Call(&sproto.AddLearnerStoreToNode{
		SetID:  1,
		NodeID: 3,
		Store:  1,
	}, &sproto.AddLearnerStoreToNodeResp{})

	//等待node3,store1启动成功
	for {
		resp, err = consoleClient.Call(&sproto.GetSetStatus{}, &sproto.GetSetStatusResp{})
		var n3 *sproto.KvnodeStatus
		if nil == err {
			for _, set := range resp.(*sproto.GetSetStatusResp).Sets {
				if set.SetID == int32(1) {
					for _, n := range set.Nodes {
						if n.NodeID == int32(3) {
							n3 = n
							break
						}
					}
				}
				if nil != n3 {
					break
				}
			}

			if len(n3.Stores) > 0 && n3.Stores[0].Progress > 0 {
				break
			}
		}
	}

	for i := 0; i < 100; i++ {
		fields := map[string]interface{}{}
		fields["age"] = 12
		name := fmt.Sprintf("sniperHW:%d", i)
		fields["name"] = name
		fields["phone"] = "123456789123456789123456789"
		assert.Nil(t, c.Set("users1", name, fields).Exec().ErrCode)
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

	resp, err = consoleClient.Call(&sproto.RemoveNodeStore{
		SetID:  1,
		NodeID: 3,
		Store:  1,
	}, &sproto.RemoveNodeStoreResp{})

	//等待node3,store1被移除
	for {
		resp, err = consoleClient.Call(&sproto.GetSetStatus{}, &sproto.GetSetStatusResp{})
		var n3 *sproto.KvnodeStatus
		if nil == err {
			for _, set := range resp.(*sproto.GetSetStatusResp).Sets {
				if set.SetID == int32(1) {
					for _, n := range set.Nodes {
						if n.NodeID == int32(3) {
							n3 = n
							break
						}
					}
				}
				if nil != n3 {
					break
				}
			}

			if len(n3.Stores) == 0 {
				break
			}
		}
	}

	//删除一个kvnode
	resp, err = consoleClient.Call(&sproto.RemNode{
		SetID:  1,
		NodeID: 3,
	}, &sproto.RemNodeResp{})

	fmt.Println(resp, err)

	node3.Stop()

	for i := 0; i < 100; i++ {
		fields := map[string]interface{}{}
		fields["age"] = 12
		name := fmt.Sprintf("sniperHW:%d", i)
		fields["name"] = name
		fields["phone"] = "123456789123456789123456789"
		assert.Nil(t, c.Set("users1", name, fields).Exec().ErrCode)
	}

	gate1.Stop()

	gateConf.ReqLimit.HardLimit = 50

	gate1, err = flygate.NewFlyGate(gateConf, "localhost:10110")

	if nil != err {
		panic(err)
	}

	stopClient := int32(0)
	pendingCount := int32(0)

	clientArray := []*client.Client{}

	for j := 0; j < 4; j++ {
		cc, _ := client.OpenClient(client.ClientConf{PD: []string{"localhost:8110"}})
		clientArray = append(clientArray, cc)

		for i := 0; i < 200; i++ {
			fields := map[string]interface{}{}
			fields["age"] = 12
			name := fmt.Sprintf("sniperHW:%d", i)
			fields["name"] = name
			fields["phone"] = "123456789123456789123456789"

			var cb func(r *client.StatusResult)
			cb = func(r *client.StatusResult) {
				atomic.AddInt32(&pendingCount, -1)
				if atomic.LoadInt32(&stopClient) == 0 {
					atomic.AddInt32(&pendingCount, 1)
					cc.Set("users1", name, fields).AsyncExec(cb)
				}
			}
			atomic.AddInt32(&pendingCount, 1)
			cc.Set("users1", name, fields).AsyncExec(cb)
		}

	}

	time.Sleep(time.Second * 5)

	gate2, err := flygate.NewFlyGate(gateConf, "localhost:10111")

	if nil != err {
		panic(err)
	}

	time.Sleep(time.Second * 5)

	gate1.Stop()

	time.Sleep(time.Second * 5)

	gate2.Stop()

	time.Sleep(time.Second * 5)

	atomic.StoreInt32(&stopClient, 1)

	for atomic.LoadInt32(&pendingCount) > 0 {

	}

	for _, v := range clientArray {
		v.Close()
	}

	for {
		//排空所有kv
		consoleClient.Call(&sproto.DrainKv{}, &sproto.DrainKvResp{})

		resp, err = consoleClient.Call(&sproto.GetSetStatus{}, &sproto.GetSetStatusResp{})
		if nil == err {
			kvcount := 0
			for _, set := range resp.(*sproto.GetSetStatusResp).Sets {
				kvcount += int(set.Kvcount)
			}
			if kvcount == 0 {
				break
			} else {
				fmt.Printf("total kvcount:%d\n", kvcount)
			}
		} else {
			fmt.Println(err)
		}
		time.Sleep(time.Second)
	}

	//清空所有db数据
	resp, err = consoleClient.Call(&sproto.ClearDBData{}, &sproto.ClearDBDataResp{})
	assert.Equal(t, resp.(*sproto.ClearDBDataResp).Ok, true)

	node1.Stop()

	node2.Stop()

	fmt.Println("Stop gate")

	fmt.Println("Stop pd")
	pd.Stop()

	pd, _ = newPD(t, pdRaftID)

	time.Sleep(time.Second * 2)

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

	pd, pdRaftID := newPD(t, 0)

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

	pd, _ = newPD(t, pdRaftID)

	time.Sleep(time.Second * 2)

	pd.Stop()

	conn.Close()
}

func TestAddSet(t *testing.T) {
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

	pd, pdRaftID := newPD(t, 0)

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

	pd, _ = newPD(t, pdRaftID)

	consoleClient := console.NewClient("localhost:8110")
	for {
		resp, err := consoleClient.Call(&sproto.GetSetStatus{}, &sproto.GetSetStatusResp{})
		nodes := []int32{}
		if nil == err {
			for _, set := range resp.(*sproto.GetSetStatusResp).Sets {
				for _, n := range set.Nodes {
					nodes = append(nodes, n.NodeID)
				}
			}

			if len(nodes) == 2 {
				break
			}
		}
	}

	pd.Stop()

}

func TestAddSet2(t *testing.T) {
	sslot.SlotCount = 128
	flypd.MinReplicaPerSet = 1
	flypd.StorePerSet = 1
	flygate.QueryRouteInfoDuration = time.Millisecond * 3 * 1000
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

	pd, pdRaftID := newPD(t, 0, "./deployment2.json")

	addr, _ := net.ResolveUDPAddr("udp", "localhost:8110")

	for {
		resp := snet.UdpCall([]*net.UDPAddr{addr},
			snet.MakeMessage(0, &sproto.AddSet{
				&sproto.DeploymentSet{
					SetID: 2,
					Nodes: []*sproto.DeploymentKvnode{
						&sproto.DeploymentKvnode{
							NodeID:      2,
							Host:        "localhost",
							ServicePort: 9211,
							RaftPort:    9221,
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
		time.Sleep(time.Second * 2)
	}

	node1, err := flykv.NewKvNode(1, false, kvConf, flykv.NewSqlDB())

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

	stopCh := make(chan struct{})

	var storeBalanced int32

	go func() {
		defer close(stopCh)
		for atomic.LoadInt32(&storeBalanced) == 0 {
			for i := 0; i < 100 && atomic.LoadInt32(&storeBalanced) == 0; i++ {
				fields := map[string]interface{}{}
				fields["age"] = 12
				name := fmt.Sprintf("sniperHW:%d", i)
				fields["name"] = name
				fields["phone"] = "123456789123456789123456789"
				e := c.Set("users1", name, fields).Exec().ErrCode
				if nil != e {
					fmt.Println(e)
				}
			}
		}
		fmt.Println("client break here")
	}()

	node2, err := flykv.NewKvNode(2, false, kvConf, flykv.NewSqlDB())

	if nil != err {
		panic(err)
	}

	//等待slot平衡
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
			slotPerStore := sslot.SlotCount / 2

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
				fmt.Println("balance ok")
				atomic.StoreInt32(&storeBalanced, 1)
				break
			}
		}
		time.Sleep(time.Second)
	}

	<-stopCh

	stopCh = make(chan struct{})

	atomic.StoreInt32(&storeBalanced, 0)

	go func() {
		defer close(stopCh)
		for atomic.LoadInt32(&storeBalanced) == 0 {
			for i := 0; i < 100 && atomic.LoadInt32(&storeBalanced) == 0; i++ {
				fields := map[string]interface{}{}
				fields["age"] = 12
				name := fmt.Sprintf("sniperHW:%d", i)
				fields["name"] = name
				fields["phone"] = "123456789123456789123456789"
				e := c.Set("users1", name, fields).Exec().ErrCode
				if nil != e {
					fmt.Println(e)
				}
			}
		}
		fmt.Println("client break here")
	}()

	for {
		resp := snet.UdpCall([]*net.UDPAddr{addr},
			snet.MakeMessage(0, &sproto.SetMarkClear{
				SetID: 2,
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
		time.Sleep(time.Second * 2)
	}

	fmt.Println("-------------rem set---------------------------")

	for {
		resp := snet.UdpCall([]*net.UDPAddr{addr},
			snet.MakeMessage(0, &sproto.RemSet{
				SetID: 2,
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
			atomic.StoreInt32(&storeBalanced, 1)
			break
		}
		time.Sleep(time.Second * 2)
	}

	<-stopCh

	gate1.Stop()
	pd.Stop()
	node1.Stop()
	node2.Stop()

	pd, _ = newPD(t, pdRaftID, "./deployment2.json")

	time.Sleep(time.Second * 2)
	pd.Stop()
}

func TestStoreBalance(t *testing.T) {
	sslot.SlotCount = 128
	flypd.MinReplicaPerSet = 1
	flypd.StorePerSet = 3
	flygate.QueryRouteInfoDuration = time.Millisecond * 3 * 1000
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

	pd, _ := newPD(t, 0, "./deployment2.json")

	node1, err := flykv.NewKvNode(1, false, kvConf, flykv.NewSqlDB())

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

	stoped := int32(0)

	c, _ := client.OpenClient(client.ClientConf{PD: []string{"localhost:8110"}})

	go func() {
		for atomic.LoadInt32(&stoped) == 0 {
			for i := 0; i < 100 && atomic.LoadInt32(&stoped) == 0; i++ {
				fields := map[string]interface{}{}
				fields["age"] = 12
				name := fmt.Sprintf("sniperHW:%d", i)
				fields["name"] = name
				fields["phone"] = "123456789123456789123456789"
				e := c.Set("users1", name, fields).Exec().ErrCode
				if nil != e {
					fmt.Println("set--------------- error:", e)
				}
			}
		}
		fmt.Println("client break here")
	}()

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

	atomic.StoreInt32(&stoped, 1)

	gate1.Stop()
	node1.Stop()
	node2.Stop()
	node3.Stop()
	pd.Stop()

}

func TestSuspendResume(t *testing.T) {
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

	pd, _ := newPD(t, 0, "./deployment2.json")

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

	logger.GetSugar().Infof("--------------------add learner 2:2 OK------------------------")

	node2, err := flykv.NewKvNode(2, true, kvConf, flykv.NewSqlDB())

	if nil != err {
		panic(err)
	}

	//suspend store1
	consoleClient := console.NewClient("localhost:8110")
	fmt.Println("--------------------wait suspend------------------------------")

	for {
		consoleClient.Call(&sproto.CpSuspendStore{SetID: 1, Store: 1}, &sproto.CpSuspendStoreResp{})
		resp, err := consoleClient.Call(&sproto.GetSetStatus{}, &sproto.GetSetStatusResp{})
		if nil == err {
			haltStoreCount := 0
			for _, set := range resp.(*sproto.GetSetStatusResp).Sets {
				for _, n := range set.Nodes {
					for _, s := range n.Stores {
						if s.Halt {
							haltStoreCount++
						}
					}
				}
			}

			if haltStoreCount == 2 {
				break
			} else {
				time.Sleep(time.Second)
			}
		}
	}

	node1.Stop()
	node2.Stop()

	node1, err = flykv.NewKvNode(1, false, kvConf, flykv.NewSqlDB())

	if nil != err {
		panic(err)
	}

	node2, err = flykv.NewKvNode(2, false, kvConf, flykv.NewSqlDB())

	if nil != err {
		panic(err)
	}

	//resume stroe1
	fmt.Println("--------------------wait resume------------------------------")

	for {
		consoleClient.Call(&sproto.CpResumeStore{SetID: 1, Store: 1}, &sproto.CpResumeStoreResp{})
		resp, err := consoleClient.Call(&sproto.GetSetStatus{}, &sproto.GetSetStatusResp{})
		if nil == err {
			haltStoreCount := 0
			for _, set := range resp.(*sproto.GetSetStatusResp).Sets {
				for _, n := range set.Nodes {
					for _, s := range n.Stores {
						if s.Halt {
							haltStoreCount++
						}
					}
				}
			}

			if haltStoreCount == 0 {
				break
			} else {
				time.Sleep(time.Second)
			}
		}
	}

	node1.Stop()
	node2.Stop()
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

	kvConf.WriteBackMode = "WriteThrough"

	pd, _ := newPD(t, 0)

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

func TestSolo(t *testing.T) {
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

	kvConf.Mode = "solo"
	kvConf.SoloConfig.RaftUrl = "1@1@http://127.0.0.1:12377@localhost:8110@voter"
	kvConf.SoloConfig.Stores = []int{1}
	kvConf.SoloConfig.MetaPath = "meta.json"

	node1, err := flykv.NewKvNode(1, false, kvConf, flykv.NewSqlDB())

	if nil != err {
		panic(err)
	}

	c, _ := client.OpenClient(client.ClientConf{SoloService: "localhost:8110", UnikeyPlacement: sslot.MakeUnikeyPlacement([]int{1})})

	for {
		r := c.Get("users1", "ak", "name").Exec()
		if errcode.GetCode(r.ErrCode) == errcode.Errcode_record_notexist {
			break
		}
	}

	for i := 0; i < 100; i++ {
		fields := map[string]interface{}{}
		fields["age"] = i
		name := fmt.Sprintf("sniperHW:%d", i)
		fields["name"] = name
		fields["phone"] = "123456789123456789123456789"
		r := c.Set("users1", name, fields).Exec()
		assert.Nil(t, r.ErrCode)
	}

	sc, _ := client.NewScanner(client.ClientConf{Stores: []int{1}, SoloService: "localhost:8110", UnikeyPlacement: sslot.MakeUnikeyPlacement([]int{1})}, "users1", []string{"name", "age", "phone"})

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

}

/*

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

*/

func TestMeta(t *testing.T) {
	sslot.SlotCount = 128
	flypd.MinReplicaPerSet = 1
	flypd.StorePerSet = 2

	os.RemoveAll("./testRaftLog")

	var err error

	dbConf := &dbconf{}
	if _, err = toml.DecodeFile("test_dbconf.toml", dbConf); nil != err {
		panic(err)
	}

	//移除table
	//先删除table1
	dbc, err := sql.SqlOpen(dbConf.DBType, dbConf.Host, dbConf.Port, dbConf.Db, dbConf.Usr, dbConf.Pwd)

	fmt.Println(err)

	sql.DropTable(dbc, &db.TableDef{
		Name:      "table2",
		DbVersion: 1,
	})

	sql.DropTable(dbc, &db.TableDef{
		Name:      "table2",
		DbVersion: 3,
	})

	pd, pdRaftID := newPD(t, 0, "./deployment2.json")

	kvConf, err := flykv.LoadConfigStr(fmt.Sprintf(flyKvConfigStr, dbConf.DBType, dbConf.Host, dbConf.Port, dbConf.Usr, dbConf.Pwd, dbConf.Db))

	if nil != err {
		panic(err)
	}

	node1, err := flykv.NewKvNode(1, false, kvConf, flykv.NewSqlDB())

	if nil != err {
		panic(err)
	}

	consoleClient := console.NewClient("localhost:8110")

	//add table
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

		resp, err := consoleClient.Call(req, &sproto.MetaAddTableResp{})
		fmt.Println(resp, err)
	}

	//remove table
	{
		req := &sproto.MetaRemoveTable{
			Table:   "table2",
			Version: 2,
		}

		resp, err := consoleClient.Call(req, &sproto.MetaRemoveTableResp{})
		fmt.Println(resp, err)

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

		resp, err := consoleClient.Call(req, &sproto.MetaAddTableResp{})
		fmt.Println(resp, err)
	}

	//add fields
	{
		req := &sproto.MetaAddFields{
			Table:   "table2",
			Version: 4,
		}

		req.Fields = append(req.Fields, &sproto.MetaFiled{
			Name:    "field2",
			Type:    "string",
			Default: "hello",
		})

		resp, err := consoleClient.Call(req, &sproto.MetaAddFieldsResp{})
		fmt.Println(resp, err)
	}

	//remove field
	{
		req := &sproto.MetaRemoveFields{
			Table:   "table2",
			Version: 5,
		}

		req.Fields = append(req.Fields, "field2")
		resp, err := consoleClient.Call(req, &sproto.MetaRemoveFieldsResp{})
		fmt.Println(resp, err)
	}

	//add field again
	{
		req := &sproto.MetaAddFields{
			Table:   "table2",
			Version: 6,
		}

		req.Fields = append(req.Fields, &sproto.MetaFiled{
			Name: "field2",
			Type: "int",
		})
		resp, err := consoleClient.Call(req, &sproto.MetaAddFieldsResp{})
		fmt.Println(resp, err)
	}

	metaVersion := int64(0)

	//get meta
	for {
		resp, _ := consoleClient.Call(&sproto.GetMeta{}, &sproto.GetMetaResp{})
		if nil != resp && resp.(*sproto.GetMetaResp).Version > 0 {
			metaVersion = resp.(*sproto.GetMetaResp).Version
			break
		}
	}

	assert.Equal(t, metaVersion, int64(7))

	pd.Stop()

	node1.Stop()

	pd, _ = newPD(t, pdRaftID)

	node1, err = flykv.NewKvNode(1, false, kvConf, flykv.NewSqlDB())

	if nil != err {
		panic(err)
	}

	metaVersion = int64(0)

	//get meta
	for {
		resp, _ := consoleClient.Call(&sproto.GetMeta{}, &sproto.GetMetaResp{})
		if nil != resp && resp.(*sproto.GetMetaResp).Version > 0 {
			metaVersion = resp.(*sproto.GetMetaResp).Version
			break
		}
	}

	assert.Equal(t, metaVersion, int64(7))

	//检查flykv meta版本号一致

	for {
		resp, _ := consoleClient.Call(&sproto.GetSetStatus{}, &sproto.GetSetStatusResp{})
		ok := true

		for _, set := range resp.(*sproto.GetSetStatusResp).Sets {
			for _, v := range set.Stores {
				fmt.Println(v.StoreID, v.MetaVersion)

				if v.MetaVersion != metaVersion {
					ok = false
					break
				}
			}
		}
		if ok {
			break
		} else {
			time.Sleep(time.Second)
		}
	}

	pd.Stop()

	node1.Stop()

}
