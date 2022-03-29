package flykv

//go test -race -covermode=atomic -v -coverprofile=../coverage.out -run=.
//go tool cover -html=../coverage.out

import (
	"fmt"
	"github.com/BurntSushi/toml"
	"github.com/jmoiron/sqlx"
	"github.com/sniperHW/flyfish/client"
	"github.com/sniperHW/flyfish/db"
	"github.com/sniperHW/flyfish/db/sql"
	"github.com/sniperHW/flyfish/logger"
	fnet "github.com/sniperHW/flyfish/pkg/net"
	"github.com/sniperHW/flyfish/pkg/raft"
	"github.com/sniperHW/flyfish/server/flypd"
	"github.com/sniperHW/flyfish/server/mock"
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
	User   string
	Pwd    string
	DB     string
	Host   string
	Port   int
}

var configStr string = `

Mode = "solo"

SnapshotCurrentCount    = 0

MainQueueMaxSize        = 10000

MaxCachePerStore        = 100               #每组最大key数量，超过数量将会触发key剔除

SqlLoadPipeLineSize     = 200                  #sql加载管道线大小

SqlLoadQueueSize        = 10000                #sql加载请求队列大小，此队列每CacheGroup一个

SqlLoaderCount          = 5
SqlUpdaterCount         = 5

ProposalFlushInterval   = 100
ReadFlushInterval       = 10 

RaftLogDir              = "testRaftLog"

RaftLogPrefix           = "flykv"

WriteBackMode           ="WriteThrough"



[SoloConfig]
RaftUrl                 = "%s"
Stores                  = [1]
MetaPath                = "test/meta.json"
                  	
[DBConfig]
DBType        = "%s"
Host          = "%s"
Port          = %d
User	      = "%s"
Password      = "%s"
DB            = "%s"



[Log]
MaxLogfileSize  = 104857600 # 100mb
LogDir          = "log"
LogPrefix       = "flykv"
LogLevel        = "info"
EnableStdout    = true	
MaxAge          = 14
MaxBackups      = 10

`

type mockBackEnd struct {
	d *mock.DB
}

func newMockDB(mockdb *mockBackEnd) *mockBackEnd {
	d := &mockBackEnd{}

	if nil == mockdb {
		d.d = mock.NewDB()
	} else {
		d.d = mockdb.d.Clone()
	}

	return d
}

func (d *mockBackEnd) start(config *Config, dbc *sqlx.DB) error {
	d.d.Start()
	return nil
}

func (d *mockBackEnd) issueLoad(l db.DBLoadTask) bool {
	return d.d.IssueTask(l) == nil
}

func (d *mockBackEnd) issueUpdate(u db.DBUpdateTask) bool {
	return d.d.IssueTask(u) == nil
}

func (d *mockBackEnd) stop() {
	d.d.Stop()
}

var config *Config
var dbConf *dbconf

var clearUsers1 func()

func init() {

	sslot.SlotCount = 1820

	raft.SnapshotCount = 100
	raft.SnapshotCatchUpEntriesN = 100

	go func() {
		http.ListenAndServe("localhost:6060", nil)
	}()

	var err error

	dbConf = &dbconf{}
	if _, err = toml.DecodeFile("test/test_dbconf.toml", dbConf); nil != err {
		panic(err)
	}

	config, err = LoadConfigStr(fmt.Sprintf(configStr, "1@0@http://127.0.0.1:12377@localhost:10018@voter", dbConf.DBType, dbConf.Host, dbConf.Port, dbConf.User, dbConf.Pwd, dbConf.DB))

	if nil != err {
		panic(err)
	}

	//清理bloomfilter
	dbc, err := sql.SqlOpen(dbConf.DBType, dbConf.Host, dbConf.Port, dbConf.DB, dbConf.User, dbConf.Pwd)
	if nil != err {
		panic(err)
	}

	clearUsers1 = func() {
		dbc.Exec("delete from users1_0;")
	}
}

func GetStore(unikey string) int {
	return 1
}

func newSqlDBBackEnd() dbI {
	return NewSqlDB()
}

func newMockDBBackEnd(mockdb *mockBackEnd) dbI {
	return newMockDB(mockdb)
}

func start1Node(id int, b dbI, join bool, cfg *Config, waitready bool) *kvnode {

	node, err := NewKvNode(uint16(id), join, cfg, b)
	if nil != err {
		panic(err)
	}

	fmt.Println("start1Node", id)

	if waitready {
		waitCondition(func() bool {
			node.muS.RLock()
			defer node.muS.RUnlock()
			for _, v := range node.stores {
				if !v.isReady() {
					return false
				}
			}
			return true
		})
	}
	return node
}

func test(t *testing.T, c *client.Client) {
	{
		dbc, _ := sql.SqlOpen(dbConf.DBType, dbConf.Host, dbConf.Port, dbConf.DB, dbConf.User, dbConf.Pwd)

		dbc.Exec("delete from users1_0 where __key__ = 'sniperHW';")

		fields := map[string]interface{}{}
		fields["age"] = 12
		fields["name"] = "sniperHW"
		fields["phone"] = []byte(strings.Repeat("a", 4096))

		rr := c.Set("users1", "sniperHW", fields).Exec()
		assert.Nil(t, rr.ErrCode)

		rr = c.Set("users1", "sniperHW", fields, 0).Exec()

		assert.Equal(t, Err_version_mismatch, rr.ErrCode)

		r := c.GetAll("users1", "sniperHW").Exec()

		assert.Equal(t, string(r.Fields["phone"].GetBlob()), strings.Repeat("a", 4096))

		assert.Nil(t, c.Kick("users1", "sniperHW").Exec().ErrCode)

		r2 := c.Del("users1", "sniperHW").Exec()
		assert.Nil(t, r2.ErrCode)

		assert.Nil(t, c.Kick("users1", "sniperHW").Exec().ErrCode)

		delete(fields, "age")

		c.Set("users1", "sniperHW", fields).Exec()

		assert.Nil(t, c.Kick("users1", "sniperHW").Exec().ErrCode)

		r = c.GetAll("users1", "sniperHW").Exec()
		assert.Nil(t, r.ErrCode)

		assert.Equal(t, r.Fields["age"].GetInt(), int64(0))

		r2 = c.Del("users1", "sniperHW").Exec()
		assert.Nil(t, r2.ErrCode)

		assert.Nil(t, c.Kick("users1", "sniperHW").Exec().ErrCode)

	}

	fields := map[string]interface{}{}
	fields["age"] = 12
	fields["name"] = "sniperHW"

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
		assert.Equal(t, r.ErrCode, Err_record_unchange)

		r1 := c.Del("users1", "sniperHW").Exec()
		assert.Nil(t, r1.ErrCode)

		r = c.GetAll("users1", "sniperHW").Exec()
		assert.Equal(t, r.ErrCode, Err_record_notexist)

		assert.Nil(t, c.Kick("users1", "sniperHW").Exec().ErrCode)

		r3 := c.Del("users1", "sniperHW").Exec()
		assert.Equal(t, r3.ErrCode, Err_record_notexist)

		r3 = c.Del("users1", "sniperHW").Exec()
		assert.Equal(t, r3.ErrCode, Err_record_notexist)

	}

	fmt.Println("-----------------------------set----------------------------------")

	{
		r1 := c.Set("users1", "sniperHW", fields).Exec()
		assert.Nil(t, r1.ErrCode)

		assert.Nil(t, c.Kick("users1", "sniperHW").Exec().ErrCode)

		r1 = c.Set("users1", "sniperHW", fields).Exec()
		assert.Nil(t, r1.ErrCode)

		time.Sleep(time.Second)

		c.Kick("users1", "sniperHW").Exec()

		r1 = c.Set("users1", "sniperHW", fields, 1).Exec()
		assert.Equal(t, r1.ErrCode, Err_version_mismatch)

		r2 := c.Del("users1", "sniperHW").Exec()
		assert.Nil(t, r2.ErrCode)

		assert.Nil(t, c.Kick("users1", "sniperHW").Exec().ErrCode)

		r1 = c.Set("users1", "sniperHW", fields).Exec()
		assert.Nil(t, r1.ErrCode)

	}

	fmt.Println("-----------------------------setNx----------------------------------")

	{
		r1 := c.SetNx("users1", "sniperHW", fields).Exec()
		assert.Equal(t, r1.ErrCode, Err_record_exist)

		r2 := c.Del("users1", "sniperHW").Exec()
		assert.Nil(t, r2.ErrCode)

		r1 = c.SetNx("users1", "sniperHW", fields).Exec()
		assert.Nil(t, r1.ErrCode)

		assert.Nil(t, c.Kick("users1", "sniperHW").Exec().ErrCode)

		r1 = c.SetNx("users1", "sniperHW", fields).Exec()
		assert.Equal(t, r1.ErrCode, Err_record_exist)

		r2 = c.Del("users1", "sniperHW").Exec()
		assert.Nil(t, r2.ErrCode)

		for {
			if nil == c.Kick("users1", "sniperHW").Exec().ErrCode {
				break
			}
			time.Sleep(time.Millisecond * 10)
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
		assert.Equal(t, r2.ErrCode, Err_cas_not_equal)

		r3 := c.Del("users1", "sniperHW").Exec()
		assert.Nil(t, r3.ErrCode)

		r2 = c.CompareAndSet("users1", "sniperHW", "age", 11, 10).Exec()
		assert.Equal(t, r2.ErrCode, Err_record_notexist)

		assert.Nil(t, c.Kick("users1", "sniperHW").Exec().ErrCode)

		r2 = c.CompareAndSet("users1", "sniperHW", "age", 11, 10).Exec()
		assert.Equal(t, r2.ErrCode, Err_record_notexist)

		assert.Nil(t, c.Kick("users1", "sniperHW").Exec().ErrCode)

		r2 = c.CompareAndSet("users1", "sniperHW", "age", 10, 11).Exec()
		assert.Equal(t, r2.ErrCode, Err_record_notexist)

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
		assert.Equal(t, r2.ErrCode, Err_cas_not_equal)

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

		fmt.Println("---------------------------------")

		assert.Nil(t, c.Kick("users1", "sniperHW").Exec().ErrCode)
	}

}

func Test1Node1Store1(t *testing.T) {

	InitLogger(logger.NewZapLogger("testRaft.log", "./log", config.Log.LogLevel, config.Log.MaxLogfileSize, config.Log.MaxAge, config.Log.MaxBackups, config.Log.EnableStdout))

	//先删除所有kv文件
	os.RemoveAll("./testRaftLog")

	clearUsers1()

	client.InitLogger(GetLogger())

	node := start1Node(1, newSqlDBBackEnd(), false, config, true)

	c, _ := client.OpenClient(client.ClientConf{SoloService: "localhost:10018", UnikeyPlacement: GetStore})

	c.Del("users1", "sniperHW").Exec()

	fields := map[string]interface{}{}
	fields["age"] = 12
	fields["name"] = "sniperHW"

	r1 := c.Set("users1", "sniperHW", fields).Exec()
	assert.Nil(t, r1.ErrCode)
	fmt.Println("version-----------", r1.Version)

	time.Sleep(time.Second)

	r1 = c.Set("users1", "sniperHW", fields).Exec()
	assert.Nil(t, r1.ErrCode)
	fmt.Println("version-----------", r1.Version)

	{
		fields := map[string]interface{}{}
		fields["age"] = 12
		fields["name"] = "sniperHW"
		fmt.Println(c.Set("users1", "sniperHW:1", fields).Exec().ErrCode)
	}

	{
		fmt.Println(c.CompareAndSetNx("users1", "sniperHW:1", "age", 12, 11).Exec().ErrCode)
	}

	{
		fmt.Println(c.CompareAndSetNx("users1", "sniperHW:1", "age", 11, 12).Exec().ErrCode)
	}

	addr1, _ := net.ResolveUDPAddr("udp", "127.0.0.1:10018")

	//清空所有kv
	conn, _ := fnet.NewUdp("localhost:0", snet.Pack, snet.Unpack)
	for {
		conn.SendTo(addr1, snet.MakeMessage(0, &sproto.DrainStore{
			Store: int32(1),
		}))

		ch := make(chan int)

		node.stores[1].mainQueue.AppendHighestPriotiryItem(func() {
			GetSugar().Infof("%v", node.stores[1].kvcount)
			ch <- node.stores[1].kvcount
		})

		<-ch

		break

		/*c := <-ch

		if 0 == c {
			break
		} else {
			GetSugar().Infof("kvcount:%d", c)
			time.Sleep(time.Second)

		}*/
	}
	conn.Close()

	{
		assert.Equal(t, c.CompareAndSetNx("users1", "sniperHW:1", "age", 11, 12).Exec().ErrCode, Err_cas_not_equal)
	}

	node.Stop()

	fmt.Println("stop ok")

	//start again
	node = start1Node(1, newSqlDBBackEnd(), false, config, true)

	time.Sleep(time.Second * 2)

	node.Stop()

	fmt.Println("stop ok")
}

func Test1Node1Store2(t *testing.T) {

	InitLogger(logger.NewZapLogger("testRaft.log", "./log", config.Log.LogLevel, config.Log.MaxLogfileSize, config.Log.MaxAge, config.Log.MaxBackups, config.Log.EnableStdout))

	//先删除所有kv文件
	os.RemoveAll("./testRaftLog")

	clearUsers1()

	client.InitLogger(GetLogger())

	node := start1Node(1, newSqlDBBackEnd(), false, config, true)

	c, _ := client.OpenClient(client.ClientConf{SoloService: "localhost:10018", UnikeyPlacement: GetStore})

	test(t, c)

	node.Stop()

	fmt.Println("stop ok")

}

func Test1Node1StoreSnapshot1(t *testing.T) {

	oldV := config.MaxCachePerStore

	config.WriteBackMode = "WriteBackOnSwap"

	config.MaxCachePerStore = 10000

	InitLogger(logger.NewZapLogger("testRaft.log", "./log", config.Log.LogLevel, config.Log.MaxLogfileSize, config.Log.MaxAge, config.Log.MaxBackups, config.Log.EnableStdout))

	//先删除所有kv文件
	os.RemoveAll("./testRaftLog")

	clearUsers1()

	client.InitLogger(GetLogger())

	node := start1Node(1, newSqlDBBackEnd(), false, config, true)

	c, _ := client.OpenClient(client.ClientConf{SoloService: "localhost:10018", UnikeyPlacement: GetStore})

	for i := 0; i < 100; i++ {
		fields := map[string]interface{}{}
		fields["age"] = 12
		name := fmt.Sprintf("sniperHW:%d", i)
		fields["name"] = name
		fields["phone"] = []byte("123456789123456789123456789")

		r := c.Set("users1", name, fields).Exec()
		assert.Nil(t, r.ErrCode)
	}

	assert.Nil(t, c.Kick("users1", "sniperHW:99").Exec().ErrCode)

	for i := 100; i < 200; i++ {
		fields := map[string]interface{}{}
		fields["age"] = 12
		name := fmt.Sprintf("sniperHW:%d", i)
		fields["name"] = name
		fields["phone"] = []byte("123456789123456789123456789")

		r := c.Set("users1", name, fields).Exec()
		assert.Nil(t, r.ErrCode)
	}

	//重新加载
	c.GetAll("users1", "sniperHW:99").Exec()

	assert.Nil(t, c.Kick("users1", "sniperHW:199").Exec().ErrCode)

	for i := 200; i < 300; i++ {
		fields := map[string]interface{}{}
		fields["age"] = 12
		name := fmt.Sprintf("sniperHW:%d", i)
		fields["name"] = name
		fields["phone"] = []byte("123456789123456789123456789")

		r := c.Set("users1", name, fields).Exec()
		assert.Nil(t, r.ErrCode)
	}

	time.Sleep(time.Second * 2)

	node.Stop()

	GetSugar().Infof("start again")

	node = start1Node(1, newSqlDBBackEnd(), false, config, true)

	time.Sleep(time.Second * 2)

	assert.Equal(t, 299, node.stores[1].kvcount)

	//assert.Nil(t, node.stores[1].kv[0]["users1:sniperHW:199"])

	//assert.NotNil(t, node.stores[1].kv[0]["users1:sniperHW:99"])

	node.Stop()

	config.MaxCachePerStore = oldV

	config.WriteBackMode = "WriteThrough"

}

func Test1Node1StoreSnapshot2(t *testing.T) {

	InitLogger(logger.NewZapLogger("testRaft.log", "./log", config.Log.LogLevel, config.Log.MaxLogfileSize, config.Log.MaxAge, config.Log.MaxBackups, config.Log.EnableStdout))

	//先删除所有kv文件
	os.RemoveAll("./testRaftLog")

	clearUsers1()

	client.InitLogger(GetLogger())

	node := start1Node(1, newSqlDBBackEnd(), false, config, true)

	c, _ := client.OpenClient(client.ClientConf{SoloService: "localhost:10018", UnikeyPlacement: GetStore})

	for i := 0; i < 50; i++ {
		fields := map[string]interface{}{}
		fields["age"] = 12
		name := fmt.Sprintf("sniperHW:%d", i)
		fields["name"] = name
		fields["phone"] = []byte("123456789123456789123456789")

		r := c.Set("users1", name, fields).Exec()
		assert.Nil(t, r.ErrCode)
	}

	assert.Nil(t, c.Kick("users1", "sniperHW:49").Exec().ErrCode)

	for i := 50; i < 100; i++ {
		fields := map[string]interface{}{}
		fields["age"] = 12
		name := fmt.Sprintf("sniperHW:%d", i)
		fields["name"] = name
		fields["phone"] = []byte("123456789123456789123456789")

		r := c.Set("users1", name, fields).Exec()
		assert.Nil(t, r.ErrCode)
	}

	time.Sleep(time.Second * 2)

	node.Stop()

	node = start1Node(1, newSqlDBBackEnd(), false, config, true)

	time.Sleep(time.Second * 2)

	node.Stop()

}

func TestUseMockDB(t *testing.T) {
	InitLogger(logger.NewZapLogger("testRaft.log", "./log", config.Log.LogLevel, config.Log.MaxLogfileSize, config.Log.MaxAge, config.Log.MaxBackups, config.Log.EnableStdout))

	//先删除所有kv文件
	os.RemoveAll("./testRaftLog")

	client.InitLogger(GetLogger())

	node := start1Node(1, newMockDBBackEnd(nil), false, config, true)

	c, _ := client.OpenClient(client.ClientConf{SoloService: "localhost:10018", UnikeyPlacement: GetStore})

	test(t, c)

	node.Stop()

	fmt.Println("stop ok")
}

func TestKick1(t *testing.T) {
	InitLogger(logger.NewZapLogger("testRaft.log", "./log", config.Log.LogLevel, config.Log.MaxLogfileSize, config.Log.MaxAge, config.Log.MaxBackups, config.Log.EnableStdout))
	GetSugar().Infof("MaxCachePerStore:%d", config.MaxCachePerStore)

	config.WriteBackMode = "WriteBackOnSwap"

	//先删除所有kv文件
	os.RemoveAll("./testRaftLog")

	client.InitLogger(GetLogger())

	db := newMockDBBackEnd(nil)

	node := start1Node(1, db, false, config, true)

	c, _ := client.OpenClient(client.ClientConf{SoloService: "localhost:10018", UnikeyPlacement: GetStore})

	for i := 0; i < 200; i++ {
		fields := map[string]interface{}{}
		fields["age"] = 12
		fields["name"] = "sniperHW"
		assert.Nil(t, c.Set("users1", fmt.Sprintf("sniperHW:%d", i), fields).Exec().ErrCode)
	}

	node.Stop()

	node = start1Node(1, newMockDBBackEnd(db.(*mockBackEnd)), false, config, true)

	time.Sleep(time.Second * 1)

	node.Stop()

	config.WriteBackMode = "WriteThrough"

	fmt.Println("stop ok")
}

func TestKick2(t *testing.T) {
	InitLogger(logger.NewZapLogger("testRaft.log", "./log", config.Log.LogLevel, config.Log.MaxLogfileSize, config.Log.MaxAge, config.Log.MaxBackups, config.Log.EnableStdout))
	GetSugar().Infof("MaxCachePerStore:%d", config.MaxCachePerStore)

	config.WriteBackMode = "WriteThrough"

	//先删除所有kv文件
	os.RemoveAll("./testRaftLog")

	client.InitLogger(GetLogger())

	db := newMockDBBackEnd(nil)

	node := start1Node(1, db, false, config, true)

	c, _ := client.OpenClient(client.ClientConf{SoloService: "localhost:10018", UnikeyPlacement: GetStore})

	for i := 0; i < 200; i++ {
		fields := map[string]interface{}{}
		fields["age"] = 12
		fields["name"] = "sniperHW"
		assert.Nil(t, c.Set("users1", fmt.Sprintf("sniperHW:%d", i), fields).Exec().ErrCode)
	}

	node.Stop()

	node = start1Node(1, newMockDBBackEnd(db.(*mockBackEnd)), false, config, true)

	time.Sleep(time.Second * 1)

	node.Stop()

	fmt.Println("stop ok")
}

func TestLinearizableRead(t *testing.T) {
	InitLogger(logger.NewZapLogger("testRaft.log", "./log", config.Log.LogLevel, config.Log.MaxLogfileSize, config.Log.MaxAge, config.Log.MaxBackups, config.Log.EnableStdout))
	GetSugar().Infof("MaxCachePerStore:%d", config.MaxCachePerStore)

	clearUsers1()

	config.LinearizableRead = true

	//先删除所有kv文件
	os.RemoveAll("./testRaftLog")

	client.InitLogger(GetLogger())

	node := start1Node(1, newSqlDBBackEnd(), false, config, true)

	c, _ := client.OpenClient(client.ClientConf{SoloService: "localhost:10018", UnikeyPlacement: GetStore})

	for i := 0; i < 100; i++ {
		fields := map[string]interface{}{}
		fields["age"] = 12
		fields["name"] = "sniperHW"
		assert.Nil(t, c.Set("users1", fmt.Sprintf("sniperHW:%d", i), fields).Exec().ErrCode)
	}

	for i := 0; i < 100; i++ {
		r := c.GetAll("users1", fmt.Sprintf("sniperHW:%d", i)).Exec()
		assert.Nil(t, r.ErrCode)
	}

	node.Stop()

	config.LinearizableRead = false
}

func TestUpdateMeta(t *testing.T) {
	InitLogger(logger.NewZapLogger("testRaft.log", "./log", config.Log.LogLevel, config.Log.MaxLogfileSize, config.Log.MaxAge, config.Log.MaxBackups, config.Log.EnableStdout))
	//先删除所有kv文件
	os.RemoveAll("./testRaftLog")
	client.InitLogger(GetLogger())
	node := start1Node(1, newMockDBBackEnd(nil), false, config, true)

	metajson := `
{
	"Version":2,
	"TableDefs":[
	{"Name":"users1",
	 "Fields":[
	 	{"Name":"name","Type":"string","DefaultValue":""},
	 	{"Name":"age","Type":"int","DefaultValue":"0"},
	 	{"Name":"phone","Type":"blob","DefaultValue":""}]
	}]
}

`

	update := &sproto.NotifyUpdateMeta{
		Store:   1,
		Version: 2,
		Meta:    []byte(metajson),
	}

	conn, err := fnet.NewUdp("localhost:0", snet.Pack, snet.Unpack)
	assert.Nil(t, err)
	addr, err := net.ResolveUDPAddr("udp", "127.0.0.1:10018")
	conn.SendTo(addr, snet.MakeMessage(0, update))

	version := int64(0)

	for atomic.LoadInt64(&version) != int64(2) {
		time.Sleep(time.Second)
		conn.SendTo(addr, snet.MakeMessage(0, update))
		node.stores[1].mainQueue.AppendHighestPriotiryItem(func() {
			atomic.StoreInt64(&version, node.stores[1].meta.GetVersion())
		})
	}

	conn.Close()
	node.Stop()
}

func TestSlotTransferOut(t *testing.T) {
	InitLogger(logger.NewZapLogger("testRaft.log", "./log", config.Log.LogLevel, config.Log.MaxLogfileSize, config.Log.MaxAge, config.Log.MaxBackups, config.Log.EnableStdout))

	//先删除所有kv文件
	os.RemoveAll("./testRaftLog")

	client.InitLogger(GetLogger())

	node := start1Node(1, newMockDBBackEnd(nil), false, config, true)

	store1 := node.stores[1]
	ch := make(chan int)

	store1.mainQueue.AppendHighestPriotiryItem(func() {
		slots := []int{}
		for k, v := range store1.slots {
			if nil != v {
				slots = append(slots, k)
			}
		}
		ch <- slots[0]
	})

	outslot := <-ch

	c, _ := client.OpenClient(client.ClientConf{SoloService: "localhost:10018", UnikeyPlacement: GetStore})

	for i := 0; i < 256; i++ {
		fields := map[string]interface{}{}
		fields["age"] = 12
		fields["name"] = "sniperHW"
		c.Set("users1", fmt.Sprintf("sniperHW:%d", i), fields).Exec()
	}

	fmt.Println("NotifySlotTransOut")

	addr, _ := net.ResolveUDPAddr("udp", "127.0.0.1:10018")

	for {
		resp := snet.UdpCall([]*net.UDPAddr{addr},
			snet.MakeMessage(0, &sproto.NotifySlotTransOut{
				Store: int32(1),
				Slot:  int32(outslot),
			}),
			time.Millisecond*50,
			func(respCh chan interface{}, r interface{}) {
				if m, ok := r.(*snet.Message); ok {
					if resp, ok := m.Msg.(*sproto.SlotTransOutOk); ok {
						select {
						case respCh <- resp:
						default:
						}
					}
				}
			})

		if resp != nil {
			assert.Equal(t, resp.(*sproto.SlotTransOutOk).Slot, int32(outslot))
			break
		}
	}

	var ok int32
	for atomic.LoadInt32(&ok) == 0 {
		time.Sleep(time.Millisecond * 100)
		node.stores[1].mainQueue.AppendHighestPriotiryItem(func() {
			if nil == node.stores[1].slots[outslot] {
				atomic.StoreInt32(&ok, 1)
			}
		})
	}

	node.Stop()

	node = start1Node(1, newMockDBBackEnd(nil), false, config, true)

	node.Stop()

}

func TestSlotTransferIn(t *testing.T) {
	InitLogger(logger.NewZapLogger("testRaft.log", "./log", config.Log.LogLevel, config.Log.MaxLogfileSize, config.Log.MaxAge, config.Log.MaxBackups, config.Log.EnableStdout))

	//先删除所有kv文件
	os.RemoveAll("./testRaftLog")

	client.InitLogger(GetLogger())

	node := start1Node(1, newMockDBBackEnd(nil), false, config, true)

	store1 := node.stores[1]

	ch := make(chan int)

	store1.mainQueue.AppendHighestPriotiryItem(func() {
		slots := []int{}
		for k, v := range store1.slots {
			if nil != v {
				slots = append(slots, k)
			}
		}

		store1.slots[slots[0]] = nil
		ch <- slots[0]
	})

	inslot := <-ch

	conn, err := fnet.NewUdp("localhost:0", snet.Pack, snet.Unpack)
	assert.Nil(t, err)

	addr, err := net.ResolveUDPAddr("udp", "127.0.0.1:10018")

	for {

		conn.SendTo(addr, snet.MakeMessage(0, &sproto.IsTransInReady{
			Store: int32(1),
			Slot:  int32(inslot),
		}))

		recvbuff := make([]byte, 256)
		_, r, _ := conn.ReadFrom(recvbuff)

		if r.(*snet.Message).Msg.(*sproto.IsTransInReadyResp).Ready {
			break
		} else {
			time.Sleep(time.Second)
		}
	}

	conn.SendTo(addr, snet.MakeMessage(0, &sproto.NotifySlotTransIn{
		Store: int32(1),
		Slot:  int32(inslot),
	}))

	recvbuff := make([]byte, 256)
	_, r, err := conn.ReadFrom(recvbuff)

	assert.Equal(t, r.(*snet.Message).Msg.(*sproto.SlotTransInOk).Slot, int32(inslot))

	//again

	conn.SendTo(addr, snet.MakeMessage(0, &sproto.NotifySlotTransIn{
		Store: int32(1),
		Slot:  int32(inslot),
	}))

	_, r, err = conn.ReadFrom(recvbuff)

	assert.Equal(t, r.(*snet.Message).Msg.(*sproto.SlotTransInOk).Slot, int32(inslot))

	conn.Close()

	var ok int32

	for atomic.LoadInt32(&ok) == 0 {
		time.Sleep(time.Second)
		node.stores[1].mainQueue.AppendHighestPriotiryItem(func() {
			if node.stores[1].slots[inslot] != nil {
				atomic.StoreInt32(&ok, 1)
			}
		})
	}

	node.Stop()

	node = start1Node(1, newMockDBBackEnd(nil), false, config, true)

	node.Stop()

}

func TestAddRemoveNode(t *testing.T) {
	InitLogger(logger.NewZapLogger("testRaft.log", "./log", config.Log.LogLevel, config.Log.MaxLogfileSize, config.Log.MaxAge, config.Log.MaxBackups, config.Log.EnableStdout))

	//先删除所有kv文件
	os.RemoveAll("./testRaftLog")

	client.InitLogger(GetLogger())

	node := start1Node(1, newMockDBBackEnd(nil), false, config, true)

	c, _ := client.OpenClient(client.ClientConf{SoloService: "localhost:10018", UnikeyPlacement: GetStore})

	test(t, c)

	addr, _ := net.ResolveUDPAddr("udp", "127.0.0.1:10018")

	for {
		resp := snet.UdpCall([]*net.UDPAddr{addr},
			snet.MakeMessage(0, &sproto.NotifyNodeStoreOp{
				Op:       int32(flypd.LearnerStore),
				NodeID:   int32(2),
				Host:     "localhost",
				RaftPort: int32(12378),
				Port:     int32(10019),
				Store:    int32(1),
				RaftID:   uint64(2)<<32 + uint64(1),
			}),
			time.Second,
			func(respCh chan interface{}, r interface{}) {
				if m, ok := r.(*snet.Message); ok {
					if resp, ok := m.Msg.(*sproto.NodeStoreOpOk); ok {
						select {
						case respCh <- resp:
						default:
						}
					}
				}
			})

		if resp != nil {
			break
		}
		time.Sleep(time.Second)
	}

	config2, err := LoadConfigStr(fmt.Sprintf(configStr, "1@0@http://127.0.0.1:12377@localhost:10018@voter,2@0@http://127.0.0.1:12378@localhost:10019@learner",
		dbConf.DBType, dbConf.Host, dbConf.Port, dbConf.User, dbConf.Pwd, dbConf.DB))

	if nil != err {
		panic(err)
	}

	db := newMockDBBackEnd(nil)

	node2 := start1Node(2, db, true, config2, false)

	//将node2提升为voter
	for {
		resp := snet.UdpCall([]*net.UDPAddr{addr},
			snet.MakeMessage(0, &sproto.NotifyNodeStoreOp{
				Op:     int32(flypd.VoterStore),
				Store:  int32(1),
				RaftID: uint64(2)<<32 + uint64(1),
			}),
			time.Second,
			func(respCh chan interface{}, r interface{}) {
				if m, ok := r.(*snet.Message); ok {
					if resp, ok := m.Msg.(*sproto.NodeStoreOpOk); ok {
						select {
						case respCh <- resp:
						default:
						}
					}
				}
			})

		if resp != nil {
			break
		}
		time.Sleep(time.Second)
	}

	addr2, _ := net.ResolveUDPAddr("udp", "127.0.0.1:10019")

	//转移leader
	for {
		conn, _ := fnet.NewUdp("localhost:0", snet.Pack, snet.Unpack)
		conn.SendTo(addr, snet.MakeMessage(0, &sproto.TrasnferLeader{
			StoreID:    int32(1),
			Transferee: uint64(2)<<32 + uint64(1),
		}))

		resp := snet.UdpCall([]*net.UDPAddr{addr2},
			snet.MakeMessage(0, &sproto.QueryLeader{
				Store: int32(1),
			}),
			time.Second,
			func(respCh chan interface{}, r interface{}) {
				if m, ok := r.(*snet.Message); ok {
					if resp, ok := m.Msg.(*sproto.QueryLeaderResp); ok {
						select {
						case respCh <- resp:
						default:
						}
					}
				}
			})

		if resp != nil && resp.(*sproto.QueryLeaderResp).Leader == int32(node2.id) {
			break
		}

		time.Sleep(time.Second)
	}

	c, _ = client.OpenClient(client.ClientConf{SoloService: "localhost:10019", UnikeyPlacement: GetStore})

	test(t, c)

	for {
		resp := snet.UdpCall([]*net.UDPAddr{addr2},
			snet.MakeMessage(0, &sproto.NotifyNodeStoreOp{
				Op:     int32(flypd.RemoveStore),
				Store:  int32(1),
				RaftID: uint64(1)<<32 + uint64(1),
			}),
			time.Second,
			func(respCh chan interface{}, r interface{}) {
				if m, ok := r.(*snet.Message); ok {
					if resp, ok := m.Msg.(*sproto.NodeStoreOpOk); ok {
						select {
						case respCh <- resp:
						default:
						}
					}
				}
			})

		if resp != nil {
			break
		}
		time.Sleep(time.Second)
	}

	node.Stop()

	test(t, c)

	node2.Stop()

	time.Sleep(time.Second)

	//node2 start again

	node2 = start1Node(2, newMockDBBackEnd(db.(*mockBackEnd)), false, config2, true)

	c, _ = client.OpenClient(client.ClientConf{SoloService: "localhost:10019", UnikeyPlacement: GetStore})

	for i := 0; i < 50; i++ {
		fields := map[string]interface{}{}
		fields["age"] = 12
		fields["name"] = "sniperHW"
		c.Set("users1", fmt.Sprintf("sniperHW:%d", i), fields).Exec()
	}

	GetSugar().Infof("---------------set ok--------------")

	{
		fields := map[string]interface{}{}
		fields["age"] = 12
		fields["name"] = "sniperHW"
		fmt.Println(c.Set("users1", "sniperHW:1", fields).Exec().ErrCode)
	}

	{
		fmt.Println(c.CompareAndSetNx("users1", "sniperHW:1", "age", 12, 11).Exec().ErrCode)
	}

	//清空所有kv
	conn, _ := fnet.NewUdp("localhost:0", snet.Pack, snet.Unpack)
	for {
		conn.SendTo(addr2, snet.MakeMessage(0, &sproto.DrainStore{
			Store: int32(1),
		}))

		ch := make(chan int)

		node2.stores[1].mainQueue.AppendHighestPriotiryItem(func() {
			ch <- node2.stores[1].kvcount
		})

		c := <-ch

		if 0 == c {
			break
		} else {
			GetSugar().Infof("kvcount:%d", c)
			time.Sleep(time.Second)

		}
	}
	conn.Close()

	{
		fmt.Println(c.CompareAndSetNx("users1", "sniperHW:1", "age", 11, 12).Exec().ErrCode)
	}

	node2.Stop()

}
