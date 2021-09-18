package flykv

//go test -covermode=count -v -coverprofile=coverage.out -run=.
//go tool cover -html=coverage.out

import (
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/sniperHW/flyfish/backend/db"
	"github.com/sniperHW/flyfish/backend/db/sql"
	"github.com/sniperHW/flyfish/client"
	"github.com/sniperHW/flyfish/logger"
	"github.com/sniperHW/flyfish/pkg/raft"
	"github.com/sniperHW/flyfish/server/flykv/metaLoader"
	mockDB "github.com/sniperHW/flyfish/server/mock/db"
	"github.com/sniperHW/flyfish/server/slot"
	"github.com/stretchr/testify/assert"
)

type dbconf struct {
	MysqlUser string
	MysqlPwd  string
	MysqlDb   string
	PgUser    string
	PgPwd     string
	PgDB      string
}

var configStr string = `

Mode = "solo"

SnapshotCurrentCount    = 1

MainQueueMaxSize        = 10000

LruCheckInterval        = 100              #每隔100ms执行一次lru剔除操作

MaxCachePerStore        = 100               #每组最大key数量，超过数量将会触发key剔除

SqlLoadPipeLineSize     = 200                  #sql加载管道线大小

SqlLoadQueueSize        = 10000                #sql加载请求队列大小，此队列每CacheGroup一个

SqlLoaderCount          = 5
SqlUpdaterCount         = 5

ProposalFlushInterval   = 100
ReadFlushInterval       = 10 


[SoloConfig]

RaftUrl                 = "http://127.0.0.1:12377"
ServiceHost             = "127.0.0.1"
ServicePort             = %d
RaftCluster             = "1@http://127.0.0.1:12377"
Stores                  = [1]

                  	
[DBConfig]
SqlType         = "%s"


DbHost          = "%s"
DbPort          = %d
DbUser			= "%s"
DbPassword      = "%s"
DbDataBase      = "%s"

ConfDbHost      = "%s"
ConfDbPort      = %d
ConfDbUser      = "%s"
ConfDbPassword  = "%s"
ConfDataBase    = "%s"


[Log]
MaxLogfileSize  = 104857600 # 100mb
LogDir          = "log"
LogPrefix       = "flyfish"
LogLevel        = "info"
EnableLogStdout = false	
`

type mockBackEnd struct {
	d *mockDB.DB
}

func newMockDB() *mockBackEnd {

	d := &mockBackEnd{
		d: mockDB.New(),
	}

	return d
}

func (d *mockBackEnd) start(config *Config) error {
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

var dbMeta *db.DbDef

var config *Config

func init() {

	go func() {
		http.ListenAndServe("localhost:6060", nil)
	}()

	dbConf := &dbconf{}
	if _, err := toml.DecodeFile("test_dbconf.toml", dbConf); nil != err {
		panic(err)
	}

	config, _ = LoadConfigStr(fmt.Sprintf(configStr, 10018, "pgsql", "localhost", 5432, dbConf.PgUser, dbConf.PgPwd, dbConf.PgDB, "localhost", 5432, dbConf.PgUser, dbConf.PgPwd, dbConf.PgDB))

	dbConfig := config.DBConfig

	dbMeta, _ = metaLoader.LoadDBMetaFromSqlJson(dbConfig.SqlType, dbConfig.ConfDbHost, dbConfig.ConfDbPort, dbConfig.ConfDataBase, dbConfig.ConfDbUser, dbConfig.ConfDbPassword)

}

func GetStore(unikey string) int {
	return 1
}

func newSqlDBBackEnd() dbbackendI {
	return NewSqlDbBackend()
}

func newMockDBBackEnd() dbbackendI {
	return newMockDB()
}

func start1Node(b dbbackendI) *kvnode {

	node := NewKvNode(1, config, dbMeta, sql.CreateDbMeta, b)

	if err := node.Start(); nil != err {
		panic(err)
	}

	waitCondition(func() bool {
		node.muS.RLock()
		defer node.muS.RUnlock()
		for _, v := range node.stores {
			if !v.isLeader() {
				return false
			}
		}
		return true
	})

	return node
}

func test(t *testing.T, c *client.Client) {
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

		r1 := c.Del("users1", "sniperHW", 1).Exec()
		assert.Equal(t, r1.ErrCode, Err_version_mismatch)

		r1 = c.Del("users1", "sniperHW", r.Version).Exec()
		assert.Nil(t, r1.ErrCode)

		r = c.GetAll("users1", "sniperHW").Exec()
		assert.Equal(t, r.ErrCode, Err_record_notexist)

		time.Sleep(time.Second)

		r2 := c.Kick("users1", "sniperHW").Exec()
		assert.Nil(t, r2.ErrCode)

		r3 := c.Del("users1", "sniperHW").Exec()
		assert.Equal(t, r3.ErrCode, Err_record_notexist)

		r3 = c.Del("users1", "sniperHW").Exec()
		assert.Equal(t, r3.ErrCode, Err_record_notexist)

	}

	fmt.Println("-----------------------------set----------------------------------")

	{
		r1 := c.Set("users1", "sniperHW", fields).Exec()
		assert.Nil(t, r1.ErrCode)

		time.Sleep(time.Second)

		r2 := c.Kick("users1", "sniperHW").Exec()
		assert.Nil(t, r2.ErrCode)

		r1 = c.Set("users1", "sniperHW", fields).Exec()
		assert.Nil(t, r1.ErrCode)

		time.Sleep(time.Second)

		r2 = c.Kick("users1", "sniperHW").Exec()
		assert.Nil(t, r2.ErrCode)

		r1 = c.Set("users1", "sniperHW", fields, 1).Exec()
		assert.Equal(t, r1.ErrCode, Err_version_mismatch)

		r2 = c.Del("users1", "sniperHW").Exec()
		assert.Nil(t, r2.ErrCode)

		time.Sleep(time.Second)

		r2 = c.Kick("users1", "sniperHW").Exec()
		assert.Nil(t, r2.ErrCode)

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

		time.Sleep(time.Second)

		r3 := c.Kick("users1", "sniperHW").Exec()
		assert.Nil(t, r3.ErrCode)

		r1 = c.SetNx("users1", "sniperHW", fields).Exec()
		assert.Equal(t, r1.ErrCode, Err_record_exist)

		r2 = c.Del("users1", "sniperHW").Exec()
		assert.Nil(t, r2.ErrCode)

		time.Sleep(time.Second)

		r3 = c.Kick("users1", "sniperHW").Exec()
		assert.Nil(t, r3.ErrCode)

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

		time.Sleep(time.Second)

		r3 = c.Kick("users1", "sniperHW").Exec()
		assert.Nil(t, r3.ErrCode)

		r2 = c.CompareAndSet("users1", "sniperHW", "age", 11, 10).Exec()
		assert.Equal(t, r2.ErrCode, Err_record_notexist)

		time.Sleep(time.Second)

		r3 = c.Kick("users1", "sniperHW").Exec()
		assert.Nil(t, r3.ErrCode)

		r2 = c.CompareAndSet("users1", "sniperHW", "age", 10, 11).Exec()
		assert.Equal(t, r2.ErrCode, Err_record_notexist)

		r4 := c.Set("users1", "sniperHW", fields).Exec()
		assert.Nil(t, r4.ErrCode)

		time.Sleep(time.Second)

		r3 = c.Kick("users1", "sniperHW").Exec()
		assert.Nil(t, r3.ErrCode)

		r2 = c.CompareAndSet("users1", "sniperHW", "age", 12, 11).Exec()
		assert.Nil(t, r2.ErrCode)

		time.Sleep(time.Second)

		r3 = c.Kick("users1", "sniperHW").Exec()
		assert.Nil(t, r3.ErrCode)

		r2 = c.CompareAndSet("users1", "sniperHW", "age", 12, 11, 1).Exec()
		assert.Equal(t, r2.ErrCode, Err_version_mismatch)

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

		time.Sleep(time.Second)

		r3 = c.Kick("users1", "sniperHW").Exec()
		assert.Nil(t, r3.ErrCode)

		r2 = c.CompareAndSetNx("users1", "sniperHW", "age", 11, 12, 1).Exec()
		assert.Equal(t, r2.ErrCode, Err_version_mismatch)

		r3 = c.Del("users1", "sniperHW").Exec()
		assert.Nil(t, r3.ErrCode)

		time.Sleep(time.Second)

		r3 = c.Kick("users1", "sniperHW").Exec()
		assert.Nil(t, r3.ErrCode)

		r2 = c.CompareAndSetNx("users1", "sniperHW", "age", 11, 12).Exec()
		assert.Nil(t, r2.ErrCode)

		time.Sleep(time.Second)

		r3 = c.Kick("users1", "sniperHW").Exec()
		assert.Nil(t, r3.ErrCode)

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

		r3 := c.Kick("users1", "sniperHW").Exec()
		assert.Nil(t, r3.ErrCode)

		r1 = c.IncrBy("users1", "sniperHW", "age", 2, 1).Exec()
		assert.Equal(t, r1.ErrCode, Err_version_mismatch)

		r2 = c.Del("users1", "sniperHW").Exec()
		assert.Nil(t, r2.ErrCode)

		time.Sleep(time.Second)
		r3 = c.Kick("users1", "sniperHW").Exec()
		assert.Nil(t, r3.ErrCode)

		r1 = c.IncrBy("users1", "sniperHW", "age", 2).Exec()
		assert.Nil(t, r1.ErrCode)
		assert.Equal(t, r1.Fields["age"].GetInt(), int64(2))

		time.Sleep(time.Second)
		r3 = c.Kick("users1", "sniperHW").Exec()
		assert.Nil(t, r3.ErrCode)

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

		r3 := c.Kick("users1", "sniperHW").Exec()
		assert.Nil(t, r3.ErrCode)

		r1 = c.DecrBy("users1", "sniperHW", "age", 2, 1).Exec()
		assert.Equal(t, r1.ErrCode, Err_version_mismatch)

		r2 = c.Del("users1", "sniperHW").Exec()
		assert.Nil(t, r2.ErrCode)

		time.Sleep(time.Second)
		r3 = c.Kick("users1", "sniperHW").Exec()
		assert.Nil(t, r3.ErrCode)

		r1 = c.DecrBy("users1", "sniperHW", "age", 2).Exec()
		assert.Nil(t, r1.ErrCode)
		assert.Equal(t, r1.Fields["age"].GetInt(), int64(-2))

		time.Sleep(time.Second)
		r3 = c.Kick("users1", "sniperHW").Exec()
		assert.Nil(t, r3.ErrCode)

		r1 = c.DecrBy("users1", "sniperHW", "age", 2).Exec()
		assert.Nil(t, r1.ErrCode)
		assert.Equal(t, r1.Fields["age"].GetInt(), int64(-4))

		time.Sleep(time.Second)
		r3 = c.Kick("users1", "sniperHW").Exec()
		assert.Nil(t, r3.ErrCode)

		fmt.Println("---------------------------------")

		time.Sleep(time.Second)
		r3 = c.Kick("users1", "sniperHW").Exec()
		assert.Nil(t, r3.ErrCode)
	}
}

func Test1Node1Store1(t *testing.T) {

	InitLogger(logger.NewZapLogger("testRaft.log", "./log", config.Log.LogLevel, 100, 14, true))

	//先删除所有kv文件
	os.RemoveAll("./log/kvnode-1-1")
	os.RemoveAll("./log/kvnode-1-1-snap")

	client.InitLogger(GetLogger())

	node := start1Node(newSqlDBBackEnd())

	c := client.OpenClient("localhost:10018")
	c.SetUnikeyPlacement(GetStore)

	c.Del("users1", "sniperHW", 1).Exec()

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

	node.Stop()

	fmt.Println("stop ok")

	//start again
	node = start1Node(newSqlDBBackEnd())

	time.Sleep(time.Second * 2)

	node.Stop()

	fmt.Println("stop ok")
}

func Test1Node1Store2(t *testing.T) {

	InitLogger(logger.NewZapLogger("testRaft.log", "./log", config.Log.LogLevel, 100, 14, true))

	//先删除所有kv文件
	os.RemoveAll("./log/kvnode-1-1")
	os.RemoveAll("./log/kvnode-1-1-snap")

	client.InitLogger(GetLogger())

	node := start1Node(newSqlDBBackEnd())

	c := client.OpenClient("localhost:10018")
	c.SetUnikeyPlacement(GetStore)

	test(t, c)

	node.Stop()

	fmt.Println("stop ok")

}

func Test1Node1StoreSnapshot1(t *testing.T) {
	DefaultSnapshotCount := raft.DefaultSnapshotCount
	SnapshotCatchUpEntriesN := raft.SnapshotCatchUpEntriesN

	raft.DefaultSnapshotCount = 100
	raft.SnapshotCatchUpEntriesN = 100

	InitLogger(logger.NewZapLogger("testRaft.log", "./log", config.Log.LogLevel, 100, 14, true))

	//先删除所有kv文件
	os.RemoveAll("./log/kvnode-1-1")
	os.RemoveAll("./log/kvnode-1-1-snap")

	client.InitLogger(GetLogger())

	node := start1Node(newMockDBBackEnd())

	c := client.OpenClient("localhost:10018")
	c.SetUnikeyPlacement(GetStore)

	for i := 0; i < 100; i++ {
		fields := map[string]interface{}{}
		fields["age"] = 12
		name := fmt.Sprintf("sniperHW:%d", i)
		fields["name"] = name
		fields["phone"] = "123456789123456789123456789"

		r := c.Set("users1", name, fields).Exec()
		assert.Nil(t, r.ErrCode)
	}
	time.Sleep(time.Second * 1)
	assert.Nil(t, c.Kick("users1", "sniperHW:99").Exec().ErrCode)

	for i := 100; i < 200; i++ {
		fields := map[string]interface{}{}
		fields["age"] = 12
		name := fmt.Sprintf("sniperHW:%d", i)
		fields["name"] = name
		fields["phone"] = "123456789123456789123456789"

		r := c.Set("users1", name, fields).Exec()
		assert.Nil(t, r.ErrCode)
	}

	//重新加载
	c.GetAll("users1", "sniperHW:99").Exec()
	time.Sleep(time.Second * 1)
	assert.Nil(t, c.Kick("users1", "sniperHW:199").Exec().ErrCode)

	for i := 200; i < 300; i++ {
		fields := map[string]interface{}{}
		fields["age"] = 12
		name := fmt.Sprintf("sniperHW:%d", i)
		fields["name"] = name
		fields["phone"] = "123456789123456789123456789"

		r := c.Set("users1", name, fields).Exec()
		assert.Nil(t, r.ErrCode)
	}

	time.Sleep(time.Second * 2)

	node.Stop()

	node = start1Node(newSqlDBBackEnd())

	time.Sleep(time.Second * 2)

	assert.Equal(t, 299, len(node.stores[1].keyvals[0].kv))

	assert.Nil(t, node.stores[1].keyvals[0].kv["users1:sniperHW:199"])

	assert.NotNil(t, node.stores[1].keyvals[0].kv["users1:sniperHW:99"])

	node.Stop()

	raft.DefaultSnapshotCount = DefaultSnapshotCount
	raft.SnapshotCatchUpEntriesN = SnapshotCatchUpEntriesN

}

func Test1Node1StoreSnapshot2(t *testing.T) {
	DefaultSnapshotCount := raft.DefaultSnapshotCount
	SnapshotCatchUpEntriesN := raft.SnapshotCatchUpEntriesN

	raft.DefaultSnapshotCount = 100
	raft.SnapshotCatchUpEntriesN = 100

	InitLogger(logger.NewZapLogger("testRaft.log", "./log", config.Log.LogLevel, 100, 14, true))

	//先删除所有kv文件
	os.RemoveAll("./log/kvnode-1-1")
	os.RemoveAll("./log/kvnode-1-1-snap")

	client.InitLogger(GetLogger())

	node := start1Node(newMockDBBackEnd())

	c := client.OpenClient("localhost:10018")
	c.SetUnikeyPlacement(GetStore)

	for i := 0; i < 50; i++ {
		fields := map[string]interface{}{}
		fields["age"] = 12
		name := fmt.Sprintf("sniperHW:%d", i)
		fields["name"] = name
		fields["phone"] = "123456789123456789123456789"

		r := c.Set("users1", name, fields).Exec()
		assert.Nil(t, r.ErrCode)
	}
	time.Sleep(time.Second * 1)
	assert.Nil(t, c.Kick("users1", "sniperHW:49").Exec().ErrCode)

	for i := 50; i < 100; i++ {
		fields := map[string]interface{}{}
		fields["age"] = 12
		name := fmt.Sprintf("sniperHW:%d", i)
		fields["name"] = name
		fields["phone"] = "123456789123456789123456789"

		r := c.Set("users1", name, fields).Exec()
		assert.Nil(t, r.ErrCode)
	}

	time.Sleep(time.Second * 2)

	node.Stop()

	node = start1Node(newSqlDBBackEnd())

	time.Sleep(time.Second * 2)

	node.Stop()

	raft.DefaultSnapshotCount = DefaultSnapshotCount
	raft.SnapshotCatchUpEntriesN = SnapshotCatchUpEntriesN

}

func TestUseMockDB(t *testing.T) {
	InitLogger(logger.NewZapLogger("testRaft.log", "./log", config.Log.LogLevel, 100, 14, true))

	//先删除所有kv文件
	os.RemoveAll("./log/kvnode-1-1")
	os.RemoveAll("./log/kvnode-1-1-snap")

	client.InitLogger(GetLogger())

	node := start1Node(newMockDBBackEnd())

	c := client.OpenClient("localhost:10018")
	c.SetUnikeyPlacement(GetStore)

	test(t, c)

	node.Stop()

	fmt.Println("stop ok")
}

func TestMakeUnikeyPlacement(t *testing.T) {
	fn := MakeUnikeyPlacement([]int{1, 2, 3, 4, 5})

	fmt.Println(fn("users1:huangwei:247100"), slot.Unikey2Slot("users1:huangwei:247100"))

}
