//go test -race -coverpkg=github.com/sniperHW/flyfish/server/flygate,github.com/sniperHW/flyfish/server/flypd,github.com/sniperHW/flyfish/server/flykv,github.com/sniperHW/flyfish/client -covermode=atomic -v -coverprofile=coverage.out -run=.
//go tool cover -html=coverage.out

package integrationTest

import (
	"fmt"
	"github.com/BurntSushi/toml"
	"github.com/sniperHW/flyfish/client"
	"github.com/sniperHW/flyfish/client/scanner"
	"github.com/sniperHW/flyfish/db"
	"github.com/sniperHW/flyfish/db/sql"
	//"github.com/sniperHW/flyfish/errcode"
	"github.com/sniperHW/flyfish/logger"
	"github.com/sniperHW/flyfish/pkg/etcd/pkg/idutil"
	fnet "github.com/sniperHW/flyfish/pkg/net"
	flygate "github.com/sniperHW/flyfish/server/flygate"
	flykv "github.com/sniperHW/flyfish/server/flykv"
	flypd "github.com/sniperHW/flyfish/server/flypd"
	console "github.com/sniperHW/flyfish/server/flypd/console/http"
	consoleHttp "github.com/sniperHW/flyfish/server/flypd/console/http"
	snet "github.com/sniperHW/flyfish/server/net"
	sproto "github.com/sniperHW/flyfish/server/proto"
	sslot "github.com/sniperHW/flyfish/server/slot"
	"github.com/stretchr/testify/assert"
	"net"
	"net/http"
	_ "net/http/pprof"
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

PD                        = "localhost:8110"

SnapshotCurrentCount      = 0

SnapshotCount             = 100
SnapshotCatchUpEntriesN   = 100

MainQueueMaxSize          = 10000

MaxCachePerStore          = 100               #每组最大key数量，超过数量将会触发key剔除

SqlLoadPipeLineSize       = 200                  #sql加载管道线大小

SqlLoadQueueSize          = 10000                #sql加载请求队列大小，此队列每CacheGroup一个

SqlLoaderCount            = 5
SqlUpdaterCount           = 5

ProposalFlushInterval     = 100
ReadFlushInterval         = 10 

RaftLogDir                = "testRaftLog/flykv"

RaftLogPrefix             = "flykv"

LinearizableRead          = true

#WriteBackMode          = "WriteThrough"

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

	go func() {
		http.ListenAndServe("localhost:8999", nil)
	}()

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
		fields["phone"] = []byte(strings.Repeat("a", 1024*17))

		assert.Nil(t, c.Set("users1", "sniperHWLargePhone", fields).Exec().ErrCode)

		fmt.Println("Get sniperHWLargePhone")
		r := c.GetAll("users1", "sniperHWLargePhone").Exec()
		assert.Nil(t, r.ErrCode)

		assert.Equal(t, string(r.Fields["phone"].GetBlob()), string(fields["phone"].([]byte)))
	}

	fmt.Println("-----------------------------1----------------------------------")

	{
		var wait sync.WaitGroup
		wait.Add(10)
		for i := 0; i < 10; i++ {
			c.Set("users1", "sniperHW", fields).AsyncExec(func(r *client.StatusResult) {
				assert.Nil(t, r.ErrCode)
				wait.Done()
			})
		}

		c.GetAll("users1", "sniperHW").AsyncExec(func(r *client.GetResult) {
			assert.Nil(t, r.ErrCode)
		})

		wait.Wait()
	}
	fmt.Println("-----------------------------get----------------------------------")
	{
		r := c.GetAll("users1", "sniperHW").Exec()
		assert.Nil(t, r.ErrCode)

		r = c.GetAllWithVersion("users1", "sniperHW", *r.Version).Exec()
		assert.Equal(t, r.ErrCode, flykv.Err_record_unchange)

		r1 := c.Del("users1", "sniperHW").Exec()
		assert.Nil(t, r1.ErrCode)

		r = c.GetAll("users1", "sniperHW").Exec()
		assert.Equal(t, r.ErrCode, flykv.Err_record_notexist)

		assert.Nil(t, c.Kick("users1", "sniperHW").Exec().ErrCode)

		r3 := c.Del("users1", "sniperHW").Exec()
		assert.Nil(t, r3.ErrCode)

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
		assert.Equal(t, r1.Value.GetInt(), int64(13))

		r2 := c.Del("users1", "sniperHW").Exec()
		assert.Nil(t, r2.ErrCode)

		r1 = c.IncrBy("users1", "sniperHW", "age", 2).Exec()
		assert.Nil(t, r1.ErrCode)
		assert.Equal(t, r1.Value.GetInt(), int64(2))

		assert.Nil(t, c.Kick("users1", "sniperHW").Exec().ErrCode)

		r2 = c.Del("users1", "sniperHW").Exec()
		assert.Nil(t, r2.ErrCode)

		assert.Nil(t, c.Kick("users1", "sniperHW").Exec().ErrCode)

		r1 = c.IncrBy("users1", "sniperHW", "age", 2).Exec()
		assert.Nil(t, r1.ErrCode)
		assert.Equal(t, r1.Value.GetInt(), int64(2))

		assert.Nil(t, c.Kick("users1", "sniperHW").Exec().ErrCode)

		r1 = c.IncrBy("users1", "sniperHW", "age", 2).Exec()
		assert.Nil(t, r1.ErrCode)
		assert.Equal(t, r1.Value.GetInt(), int64(4))

	}

	fmt.Println("-----------------------------mget----------------------------------")

	{
		r := client.MGet(c.GetAll("users1", "sniperHW"), c.GetAll("users1", "sniperHW21"), c.GetAll("users1", "sniperHW2")).Exec()
		assert.Equal(t, 3, len(r))
	}
}

func clearUsers1() {
	dbConf := &dbconf{}
	if _, err := toml.DecodeFile("test_dbconf.toml", dbConf); nil != err {
		panic(err)
	}

	//清理bloomfilter
	dbc, err := sql.SqlOpen(dbConf.DBType, dbConf.Host, dbConf.Port, dbConf.Db, dbConf.Usr, dbConf.Pwd)
	if nil != err {
		panic(err)
	}

	dbc.Exec("delete from users1_0;")

	dbc.Close()
}

func TestFlygate(t *testing.T) {
	sslot.SlotCount = 128
	flypd.MinReplicaPerSet = 1
	flypd.StorePerSet = 6

	os.RemoveAll("./testRaftLog")

	clearUsers1()

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

	c, _ := client.New(client.ClientConf{
		ClientType: client.FlyGate,
		PD:         []string{"localhost:8110"},
	})

	defer c.Close()

	for i := 0; i < 100; i++ {
		fields := map[string]interface{}{}
		fields["age"] = 12
		name := fmt.Sprintf("sniperHW:%d", i)
		fields["name"] = name
		fields["phone"] = []byte("123456789123456789123456789")
		assert.Nil(t, c.Set("users1", name, fields).Exec().ErrCode)
	}

	fmt.Println("----------------------run client------------------")

	testkv(t, c)

	consoleClient := console.NewClient("localhost:8110")

	//添加kvnode3
	resp, err := consoleClient.Call(&sproto.AddNode{
		SetID:       1,
		NodeID:      3,
		Host:        "localhost",
		ServicePort: 9320,
		RaftPort:    9321,
	}, &sproto.AddNodeResp{})

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

	resp, err = consoleClient.Call(&sproto.GetKvStatus{}, &sproto.GetKvStatusResp{})
	if nil == err {
		assert.Equal(t, true, resp.(*sproto.GetKvStatusResp).Kvcount > 0)
	} else {
		fmt.Println(err)
	}

	//等待node3至少有一个leader
	for {
		resp, err = consoleClient.Call(&sproto.GetKvStatus{}, &sproto.GetKvStatusResp{})
		if ok := func() bool {
			leaderCount := 0
			if nil == err {
				for _, set := range resp.(*sproto.GetKvStatusResp).Sets {
					if set.SetID == int32(1) {
						for _, n := range set.Nodes {
							if n.NodeID == int32(3) {
								for _, s := range n.Stores {
									if s.IsLeader {
										leaderCount++
									}
								}
							}
						}
					}
				}
			}
			return leaderCount == 3
		}(); ok {
			break
		} else {
			time.Sleep(time.Second)
		}
	}

	logger.GetSugar().Infof("---------------------------------------------------------")

	for i := 0; i < 100; i++ {
		fields := map[string]interface{}{}
		fields["age"] = 12
		name := fmt.Sprintf("sniperHW:%d", i)
		fields["name"] = name
		fields["phone"] = []byte("123456789123456789123456789")
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

	//删除kvnode3
	resp, err = consoleClient.Call(&sproto.RemNode{
		SetID:  1,
		NodeID: 3,
	}, &sproto.RemNodeResp{})

	//等待node3被移除
	for {
		resp, err = consoleClient.Call(&sproto.GetKvStatus{}, &sproto.GetKvStatusResp{})
		if nil == err {
			find := false
			for _, set := range resp.(*sproto.GetKvStatusResp).Sets {
				if set.SetID == int32(1) {
					for _, n := range set.Nodes {
						if n.NodeID == int32(3) {
							find = true
							break
						}
					}
				}
			}

			if !find {
				break
			}
		}
	}

	node3.Stop()

	logger.GetSugar().Infof("---------------------------run again----------------------------")

	for i := 0; i < 100; i++ {
		fields := map[string]interface{}{}
		fields["age"] = 12
		name := fmt.Sprintf("sniperHW:%d", i)
		fields["name"] = name
		fields["phone"] = []byte("123456789123456789123456789")
		//assert.Nil(t, c.Set("users1", name, fields).Exec().ErrCode)
		if nil != c.Set("users1", name, fields).Exec().ErrCode {
			panic("here")
		}
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
		cc, _ := client.New(client.ClientConf{
			ClientType: client.FlyGate,
			PD:         []string{"localhost:8110"},
		})
		clientArray = append(clientArray, cc)

		for i := 0; i < 200; i++ {
			fields := map[string]interface{}{}
			fields["age"] = 12
			name := fmt.Sprintf("sniperHW:%d", i)
			fields["name"] = name
			fields["phone"] = []byte("123456789123456789123456789")

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
		time.Sleep(time.Second)
		//logger.GetSugar().Infof("pendingCount:%d", atomic.LoadInt32(&pendingCount))
	}

	for _, v := range clientArray {
		v.Close()
	}

	for {
		//排空所有kv
		consoleClient.Call(&sproto.ClearCache{}, &sproto.ClearCacheResp{})

		resp, err = consoleClient.Call(&sproto.GetKvStatus{}, &sproto.GetKvStatusResp{})
		if nil == err {
			if resp.(*sproto.GetKvStatusResp).Kvcount == 0 {
				break
			} else {
				fmt.Printf("total kvcount:%d\n", resp.(*sproto.GetKvStatusResp).Kvcount)
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

func TestFlykv(t *testing.T) {
	sslot.SlotCount = 128
	flypd.MinReplicaPerSet = 1
	flypd.StorePerSet = 6

	os.RemoveAll("./testRaftLog")

	clearUsers1()

	pd, _ := newPD(t, 0)

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

	c, _ := client.New(client.ClientConf{
		ClientType: client.FlyKv,
		PD:         []string{"localhost:8110"},
	})

	defer c.Close()

	logger.GetSugar().Infof("----------------------run client1------------------")

	for i := 0; i < 100; i++ {
		fields := map[string]interface{}{}
		fields["age"] = 12
		name := fmt.Sprintf("sniperHW:%d", i)
		fields["name"] = name
		fields["phone"] = []byte("123456789123456789123456789")
		assert.Nil(t, c.Set("users1", name, fields).Exec().ErrCode)

	}

	testkv(t, c)

	consoleClient := console.NewClient("localhost:8110")

	//添加kvnode3
	resp, err := consoleClient.Call(&sproto.AddNode{
		SetID:       1,
		NodeID:      3,
		Host:        "localhost",
		ServicePort: 9320,
		RaftPort:    9321,
	}, &sproto.AddNodeResp{})

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

	resp, err = consoleClient.Call(&sproto.GetKvStatus{}, &sproto.GetKvStatusResp{})
	if nil == err {
		assert.Equal(t, true, resp.(*sproto.GetKvStatusResp).Kvcount > 0)
	} else {
		fmt.Println(err)
	}

	//等待node3至少有一个leader
	for {
		resp, err = consoleClient.Call(&sproto.GetKvStatus{}, &sproto.GetKvStatusResp{})
		if ok := func() bool {
			leaderCount := 0
			if nil == err {
				for _, set := range resp.(*sproto.GetKvStatusResp).Sets {
					if set.SetID == int32(1) {
						for _, n := range set.Nodes {
							if n.NodeID == int32(3) {
								for _, s := range n.Stores {
									if s.IsLeader {
										leaderCount++
									}
								}
							}
						}
					}
				}
			}
			return leaderCount == 3
		}(); ok {
			break
		} else {
			time.Sleep(time.Second)
		}
	}

	logger.GetSugar().Infof("----------------------run client2------------------")

	for i := 0; i < 100; i++ {
		fields := map[string]interface{}{}
		fields["age"] = 12
		name := fmt.Sprintf("sniperHW:%d", i)
		fields["name"] = name
		fields["phone"] = []byte("123456789123456789123456789")
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

	//删除kvnode3
	resp, err = consoleClient.Call(&sproto.RemNode{
		SetID:  1,
		NodeID: 3,
	}, &sproto.RemNodeResp{})

	//等待node3被移除
	for {
		resp, err = consoleClient.Call(&sproto.GetKvStatus{}, &sproto.GetKvStatusResp{})
		if nil == err {
			find := false
			for _, set := range resp.(*sproto.GetKvStatusResp).Sets {
				if set.SetID == int32(1) {
					for _, n := range set.Nodes {
						if n.NodeID == int32(3) {
							find = true
							break
						}
					}
				}
			}

			if !find {
				break
			}
		}
	}

	node3.Stop()

	logger.GetSugar().Infof("----------------------run client3------------------")

	for i := 0; i < 100; i++ {
		fields := map[string]interface{}{}
		fields["age"] = 12
		name := fmt.Sprintf("sniperHW:%d", i)
		fields["name"] = name
		fields["phone"] = []byte("123456789123456789123456789")
		assert.Nil(t, c.Set("users1", name, fields).Exec().ErrCode)
	}

	for {
		//排空所有kv
		consoleClient.Call(&sproto.ClearCache{}, &sproto.ClearCacheResp{})

		resp, err = consoleClient.Call(&sproto.GetKvStatus{}, &sproto.GetKvStatusResp{})
		if nil == err {
			if resp.(*sproto.GetKvStatusResp).Kvcount == 0 {
				break
			} else {
				fmt.Printf("total kvcount:%d\n", resp.(*sproto.GetKvStatusResp).Kvcount)
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

	pd.Stop()

}

func TestAddRemoveNode1(t *testing.T) {
	sslot.SlotCount = 128
	flypd.MinReplicaPerSet = 1
	flypd.StorePerSet = 3
	os.RemoveAll("./testRaftLog")

	clearUsers1()

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
		resp, _ := snet.UdpCall([]*net.UDPAddr{addr},
			&sproto.AddNode{
				SetID:       1,
				NodeID:      3,
				Host:        "localhost",
				ServicePort: 9320,
				RaftPort:    9321,
			},
			&sproto.AddNodeResp{},
			time.Second)
		if resp != nil && resp.(*sproto.AddNodeResp).Ok {
			break
		}
		time.Sleep(time.Second)
	}

	logger.GetSugar().Infof("start node1")

	node1, err := flykv.NewKvNode(1, false, kvConf, flykv.NewSqlDB())

	if nil != err {
		panic(err)
	}

	logger.GetSugar().Infof("start node2")

	node3, err := flykv.NewKvNode(3, true, kvConf, flykv.NewSqlDB())

	if nil != err {
		panic(err)
	}

	//remove node
	logger.GetSugar().Infof("remove node")

	for {
		resp, _ := snet.UdpCall([]*net.UDPAddr{addr},
			&sproto.RemNode{
				SetID:  1,
				NodeID: 3,
			},
			&sproto.RemNodeResp{},
			time.Second)
		if resp != nil {
			if resp.(*sproto.RemNodeResp).Reason == "node not found" {
				break
			} else {
				logger.GetSugar().Infof("%v", resp.(*sproto.RemNodeResp).Reason)
			}

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

func TestAddRemoveNode2(t *testing.T) {
	sslot.SlotCount = 128
	flypd.MinReplicaPerSet = 1
	flypd.StorePerSet = 3
	os.RemoveAll("./testRaftLog")

	clearUsers1()

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
		resp, _ := snet.UdpCall([]*net.UDPAddr{addr},
			&sproto.AddNode{
				SetID:       1,
				NodeID:      3,
				Host:        "localhost",
				ServicePort: 9320,
				RaftPort:    9321,
			},
			&sproto.AddNodeResp{},
			time.Second)

		if resp != nil && resp.(*sproto.AddNodeResp).Ok {
			break
		}
		time.Sleep(time.Second)
	}

	logger.GetSugar().Infof("start node1")

	node1, err := flykv.NewKvNode(1, false, kvConf, flykv.NewSqlDB())

	if nil != err {
		panic(err)
	}

	logger.GetSugar().Infof("start node2")

	node3, err := flykv.NewKvNode(3, true, kvConf, flykv.NewSqlDB())

	if nil != err {
		panic(err)
	}

	//等待node3所有store成为voter
	for {
		c := consoleHttp.NewClient("localhost:8110")
		resp, err := c.Call(&sproto.GetKvStatus{}, &sproto.GetKvStatusResp{})
		if nil != err {
			logger.GetSugar().Errorf("%v", err)
		} else {
			ok := func() bool {
				s := resp.(*sproto.GetKvStatusResp).Sets[0]
				for _, v := range s.Nodes {
					if int(v.NodeID) == 3 {
						for _, vv := range v.Stores {
							if vv.StoreType != int32(flypd.VoterStore) {
								return false
							}
						}
						return len(v.Stores) == flypd.StorePerSet
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

	//remove node
	logger.GetSugar().Infof("remove node")

	for {
		resp, _ := snet.UdpCall([]*net.UDPAddr{addr},
			&sproto.RemNode{
				SetID:  1,
				NodeID: 3,
			},
			&sproto.RemNodeResp{},
			time.Second)
		if resp != nil {
			if resp.(*sproto.RemNodeResp).Reason == "node not found" {
				break
			} else {
				logger.GetSugar().Infof("%v", resp.(*sproto.RemNodeResp).Reason)
			}

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
	clearUsers1()

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
		resp, _ := snet.UdpCall([]*net.UDPAddr{addr},
			&sproto.AddSet{
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
			},
			&sproto.AddSetResp{},
			time.Second)
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
		resp, _ := snet.UdpCall([]*net.UDPAddr{addr},
			&sproto.GetKvStatus{},
			&sproto.GetKvStatusResp{},
			time.Second)

		if resp != nil {
			slotPerStore := sslot.SlotCount / 3

			ok := true
			for _, v := range resp.(*sproto.GetKvStatusResp).Sets {
				for _, vv := range v.Stores {
					if int(vv.Slotcount) > slotPerStore+1 {
						ok = false
					}
				}
			}

			if ok {
				break
			}
		}
		time.Sleep(time.Second)
	}

	for {
		resp, _ := snet.UdpCall([]*net.UDPAddr{addr},
			&sproto.SetMarkClear{
				SetID: 3,
			},
			&sproto.SetMarkClearResp{},
			time.Second)

		if resp != nil {
			if resp.(*sproto.SetMarkClearResp).Reason == "already mark clear" {
				break
			} else {
				logger.GetSugar().Infof("%v", resp.(*sproto.SetMarkClearResp).Reason)
			}
		}
		time.Sleep(time.Second)
	}

	//等待
	for {
		resp, _ := snet.UdpCall([]*net.UDPAddr{addr},
			&sproto.GetKvStatus{},
			&sproto.GetKvStatusResp{},
			time.Second)

		if resp != nil {
			var set1 *sproto.SetStatus
			var set2 *sproto.SetStatus
			var set3 *sproto.SetStatus
			for _, v := range resp.(*sproto.GetKvStatusResp).Sets {
				if v.SetID == 1 {
					set1 = v
				} else if v.SetID == 2 {
					set2 = v
				} else if v.SetID == 3 {
					set3 = v
				}
			}

			slots1 := int(set1.Stores[0].Slotcount)
			slots2 := int(set2.Stores[0].Slotcount)
			slots3 := int(set3.Stores[0].Slotcount)

			if slots3 == 0 && slots1+slots2 == sslot.SlotCount {
				break
			}
		}
		time.Sleep(time.Second)
	}

	for {
		resp, _ := snet.UdpCall([]*net.UDPAddr{addr},
			&sproto.RemSet{
				SetID: 3,
			},
			&sproto.RemSetResp{},
			time.Second)

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
		resp, err := consoleClient.Call(&sproto.GetKvStatus{}, &sproto.GetKvStatusResp{})
		nodes := []int32{}
		if nil == err {
			for _, set := range resp.(*sproto.GetKvStatusResp).Sets {
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

func testAddSet2(t *testing.T, clientType client.ClientType) {
	sslot.SlotCount = 128
	flypd.MinReplicaPerSet = 1
	flypd.StorePerSet = 1
	flygate.QueryRouteInfoDuration = time.Millisecond * 3 * 1000
	os.RemoveAll("./testRaftLog")
	clearUsers1()
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
		resp, _ := snet.UdpCall([]*net.UDPAddr{addr},
			&sproto.AddSet{
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
			},
			&sproto.AddSetResp{},
			time.Second)

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

	c, _ := client.New(client.ClientConf{
		ClientType: clientType,
		PD:         []string{"localhost:8110"},
	})

	defer c.Close()

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
				fields["phone"] = []byte("123456789123456789123456789")
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
		resp, _ := snet.UdpCall([]*net.UDPAddr{addr},
			&sproto.GetKvStatus{},
			&sproto.GetKvStatusResp{},
			time.Second)

		if resp != nil {
			slotPerStore := sslot.SlotCount / 2

			ok := true
			for _, v := range resp.(*sproto.GetKvStatusResp).Sets {
				for _, vv := range v.Stores {
					if int(vv.Slotcount) > slotPerStore+1 {
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
				fields["phone"] = []byte("123456789123456789123456789")
				e := c.Set("users1", name, fields).Exec().ErrCode
				if nil != e {
					fmt.Println(e)
				}
			}
		}
		fmt.Println("client break here")
	}()

	for {
		resp, _ := snet.UdpCall([]*net.UDPAddr{addr},
			&sproto.SetMarkClear{
				SetID: 2,
			},
			&sproto.SetMarkClearResp{},
			time.Second)

		if resp != nil && resp.(*sproto.SetMarkClearResp).Reason == "already mark clear" {
			break
		}
		time.Sleep(time.Second * 2)
	}

	fmt.Println("-------------rem set---------------------------")

	for {
		resp, _ := snet.UdpCall([]*net.UDPAddr{addr},
			&sproto.RemSet{
				SetID: 2,
			},
			&sproto.RemSetResp{},
			time.Second)

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

func TestAddSet2Flygate(t *testing.T) {
	testAddSet2(t, client.FlyGate)
}

func TestAddSet3(t *testing.T) {
	testAddSet2(t, client.FlyKv)
}

func testStoreBalance(t *testing.T, clientType client.ClientType) {
	sslot.SlotCount = 128
	flypd.MinReplicaPerSet = 1
	flypd.StorePerSet = 3
	flygate.QueryRouteInfoDuration = time.Millisecond * 3 * 1000
	os.RemoveAll("./log")
	os.RemoveAll("./testRaftLog")
	clearUsers1()

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

	c, _ := client.New(client.ClientConf{
		ClientType: clientType,
		PD:         []string{"localhost:8110"},
	})

	defer c.Close()

	go func() {
		for atomic.LoadInt32(&stoped) == 0 {
			for i := 0; i < 100 && atomic.LoadInt32(&stoped) == 0; i++ {
				fields := map[string]interface{}{}
				fields["age"] = 12
				name := fmt.Sprintf("sniperHW:%d", i)
				fields["name"] = name
				fields["phone"] = []byte("123456789123456789123456789")
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
		resp, _ := snet.UdpCall([]*net.UDPAddr{addr},
			&sproto.AddNode{
				SetID:       1,
				NodeID:      2,
				Host:        "localhost",
				ServicePort: 9220,
				RaftPort:    9221,
			},
			&sproto.AddNodeResp{},
			time.Second)

		if resp != nil && resp.(*sproto.AddNodeResp).Ok {
			break
		}
		time.Sleep(time.Second)
	}

	node2, err := flykv.NewKvNode(2, true, kvConf, flykv.NewSqlDB())

	if nil != err {
		panic(err)
	}

	//增加node3
	for {
		resp, _ := snet.UdpCall([]*net.UDPAddr{addr},
			&sproto.AddNode{
				SetID:       1,
				NodeID:      3,
				Host:        "localhost",
				ServicePort: 9320,
				RaftPort:    9321,
			},
			&sproto.AddNodeResp{},
			time.Second)

		if resp != nil && resp.(*sproto.AddNodeResp).Ok {
			break
		}
		time.Sleep(time.Second)
	}

	node3, err := flykv.NewKvNode(3, true, kvConf, flykv.NewSqlDB())

	if nil != err {
		panic(err)
	}

	/////////////////

	for {
		c := consoleHttp.NewClient("localhost:8110")
		resp, err := c.Call(&sproto.GetKvStatus{}, &sproto.GetKvStatusResp{})
		if nil != err {
			logger.GetSugar().Errorf("%v", err)
		} else {
			ok := func() bool {
				leaderCount := []int{0, 0, 0, 0}
				for _, v := range resp.(*sproto.GetKvStatusResp).Sets {
					if v.SetID == int32(1) {
						for _, vv := range v.Nodes {
							for _, vvv := range vv.Stores {
								if vvv.IsLeader {
									leaderCount[int(vv.NodeID)]++
								}
							}
						}
					}
				}

				logger.GetSugar().Infof("leaderCount %v", leaderCount)

				for i := 1; i < len(leaderCount); i++ {
					if leaderCount[i] != 1 {
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

	atomic.StoreInt32(&stoped, 1)

	gate1.Stop()
	node1.Stop()
	node2.Stop()
	node3.Stop()
	pd.Stop()
}

func TestStoreBalanceFlygate(t *testing.T) {
	testStoreBalance(t, client.FlyGate)
}

func TestStoreBalanceFlykv(t *testing.T) {
	testStoreBalance(t, client.FlyKv)
}

func TestSuspendResume(t *testing.T) {
	sslot.SlotCount = 128
	flypd.MinReplicaPerSet = 1
	flypd.StorePerSet = 1
	os.RemoveAll("./testRaftLog")
	clearUsers1()
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
		resp, _ := snet.UdpCall([]*net.UDPAddr{addr},
			&sproto.AddNode{
				SetID:       1,
				NodeID:      2,
				Host:        "localhost",
				ServicePort: 9220,
				RaftPort:    9221,
			},
			&sproto.AddNodeResp{},
			time.Second)

		if resp != nil && resp.(*sproto.AddNodeResp).Ok {
			break
		}
		time.Sleep(time.Second)
	}

	node2, err := flykv.NewKvNode(2, true, kvConf, flykv.NewSqlDB())

	if nil != err {
		panic(err)
	}

	//等待node2所有store成为voter

	for {
		c := consoleHttp.NewClient("localhost:8110")
		resp, err := c.Call(&sproto.GetKvStatus{}, &sproto.GetKvStatusResp{})
		if nil != err {
			logger.GetSugar().Errorf("%v", err)
		} else {
			ok := func() bool {
				s := resp.(*sproto.GetKvStatusResp).Sets[0]
				for _, v := range s.Nodes {
					if int(v.NodeID) == 2 {
						for _, vv := range v.Stores {
							if vv.StoreType != int32(flypd.VoterStore) {
								return false
							}
						}
						return len(v.Stores) == flypd.StorePerSet
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

	c, _ := client.New(client.ClientConf{
		ClientType: client.FlyGate,
		PD:         []string{"localhost:8110"},
	})

	defer c.Close()

	for i := 0; i < 100; i++ {
		fields := map[string]interface{}{}
		fields["age"] = 12
		name := fmt.Sprintf("sniperHW:%d", i)
		fields["name"] = name
		fields["phone"] = []byte("123456789123456789123456789")
		e := c.Set("users1", name, fields).Exec().ErrCode
		if nil != e {
			fmt.Println("set--------------- error:", e)
		}
	}

	consoleClient := console.NewClient("localhost:8110")

	fmt.Println("--------------------wait suspend------------------------------")

	for {
		consoleClient.Call(&sproto.SuspendKvStore{}, &sproto.SuspendKvStoreResp{})
		resp, err := consoleClient.Call(&sproto.GetKvStatus{}, &sproto.GetKvStatusResp{})
		if nil == err {
			if func() bool {
				for _, set := range resp.(*sproto.GetKvStatusResp).Sets {
					for _, store := range set.Stores {
						if !store.Halt {
							return false
						}
					}
				}
				return true
			}() {
				break
			} else {
				time.Sleep(time.Second)
			}
		}
	}

	fmt.Println("--------------------wait clear cache------------------------------")

	for {
		consoleClient.Call(&sproto.ClearCache{}, &sproto.ClearCacheResp{})
		resp, err := consoleClient.Call(&sproto.GetKvStatus{}, &sproto.GetKvStatusResp{})
		if nil == err {
			if func() bool {
				for _, set := range resp.(*sproto.GetKvStatusResp).Sets {
					for _, store := range set.Stores {
						if store.Kvcount > 0 {
							return false
						}
					}
				}
				return true
			}() {
				break
			} else {
				time.Sleep(time.Second)
			}
		}
	}

	gate1.Stop()
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
		consoleClient.Call(&sproto.ResumeKvStore{}, &sproto.ResumeKvStoreResp{})
		resp, err := consoleClient.Call(&sproto.GetKvStatus{}, &sproto.GetKvStatusResp{})
		if nil == err {
			if nil == err {
				if func() bool {
					for _, set := range resp.(*sproto.GetKvStatusResp).Sets {
						for _, store := range set.Stores {
							if store.Halt {
								return false
							}
						}
					}
					return true
				}() {
					break
				} else {
					time.Sleep(time.Second)
				}
			}
		}
	}

	node1.Stop()
	node2.Stop()
	pd.Stop()
}

func testScan(t *testing.T, clientType client.ClientType) {
	sslot.SlotCount = 128
	flypd.MinReplicaPerSet = 1
	flypd.StorePerSet = 1
	os.RemoveAll("./testRaftLog")
	clearUsers1()
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

	c, _ := client.New(client.ClientConf{
		ClientType: clientType,
		PD:         []string{"localhost:8110"},
	})

	for i := 0; i < 100; i++ {
		fields := map[string]interface{}{}
		fields["age"] = i
		name := fmt.Sprintf("sniperHW:%d", i)
		fields["name"] = name
		fields["phone"] = []byte("123456789123456789123456789")
		r := c.Set("users1", name, fields).Exec()
		assert.Nil(t, r.ErrCode)
	}

	fmt.Println("set ok")

	sc, _ := scanner.New(scanner.ScannerConf{
		ScannerType: scanner.ScannerType(clientType),
		PD:          []string{"localhost:8110"},
	}, "users1", []string{"name", "age", "phone"})

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

	sc, _ = scanner.New(scanner.ScannerConf{
		ScannerType: scanner.FlyGate,
		PD:          []string{"localhost:8110"},
	}, "users1", []string{"name", "age", "phone"})

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

func TestScanFlygate(t *testing.T) {
	testScan(t, client.FlyGate)
}

func TestScanFlykv(t *testing.T) {
	testScan(t, client.FlyKv)
}

func TestMeta(t *testing.T) {
	sslot.SlotCount = 128
	flypd.MinReplicaPerSet = 1
	flypd.StorePerSet = 2

	os.RemoveAll("./testRaftLog")
	clearUsers1()
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
		resp, _ := consoleClient.Call(&sproto.GetKvStatus{}, &sproto.GetKvStatusResp{})
		ok := true

		for _, set := range resp.(*sproto.GetKvStatusResp).Sets {
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
