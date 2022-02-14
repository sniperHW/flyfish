package flykv

//go test -covermode=count -v -coverprofile=../coverage.out -run=.
//go tool cover -html=../coverage.out

import (
	"fmt"
	"github.com/BurntSushi/toml"
	"github.com/jmoiron/sqlx"
	"github.com/sniperHW/flyfish/client"
	"github.com/sniperHW/flyfish/db"
	"github.com/sniperHW/flyfish/logger"
	fnet "github.com/sniperHW/flyfish/pkg/net"
	"github.com/sniperHW/flyfish/pkg/raft"
	"github.com/sniperHW/flyfish/server/mock"
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

DBType                  = "pgsql"

SnapshotCurrentCount    = 1

MainQueueMaxSize        = 10000

LruCheckInterval        = 1000              #每隔100ms执行一次lru剔除操作

MaxCachePerStore        = 100               #每组最大key数量，超过数量将会触发key剔除

SqlLoadPipeLineSize     = 200                  #sql加载管道线大小

SqlLoadQueueSize        = 10000                #sql加载请求队列大小，此队列每CacheGroup一个

SqlLoaderCount          = 5
SqlUpdaterCount         = 5

ProposalFlushInterval   = 100
ReadFlushInterval       = 10 

RaftLogDir              = "testRaftLog"

RaftLogPrefix           = "flykv"


[SoloConfig]

RaftUrl                 = "http://127.0.0.1:12377"
ServiceHost             = "127.0.0.1"
ServicePort             = %d
Stores                  = [1]
MetaPath                = "./meta.json"

                  	
[DBConfig]

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

func newMockDB() *mockBackEnd {

	d := &mockBackEnd{
		d: mock.NewDB(),
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

func init() {
	sslot.SlotCount = 128

	go func() {
		http.ListenAndServe("localhost:6060", nil)
	}()

	var err error

	dbConf := &dbconf{}
	if _, err = toml.DecodeFile("test_dbconf.toml", dbConf); nil != err {
		panic(err)
	}

	config, err = LoadConfigStr(fmt.Sprintf(configStr, 10018, "localhost", 5432, dbConf.PgUser, dbConf.PgPwd, dbConf.PgDB))
	if nil != err {
		panic(err)
	}
}

func GetStore(unikey string) int {
	return 1
}

func newSqlDBBackEnd() dbI {
	return NewSqlDB()
}

func newMockDBBackEnd() dbI {
	return newMockDB()
}

func start1Node(b dbI) *kvnode {

	node, err := NewKvNode(1, false, config, b)

	if nil != err {
		panic(err)
	}

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

	/*fmt.Println("-----------------------------get----------------------------------")
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

		for {
			if nil == c.Kick("users1", "sniperHW").Exec().ErrCode {
				break
			}
			time.Sleep(time.Second)
		}

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
		assert.Equal(t, r1.ErrCode, Err_version_mismatch)

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

	}*/

	fmt.Println("-----------------------------setNx----------------------------------")

	{
		r1 := c.SetNx("users1", "sniperHW", fields).Exec()
		assert.Equal(t, r1.ErrCode, Err_record_exist)

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
		assert.Equal(t, r1.ErrCode, Err_record_exist)

		r2 = c.Del("users1", "sniperHW").Exec()
		assert.Nil(t, r2.ErrCode)

		/*time.Sleep(time.Second)

		for {
			if nil == c.Kick("users1", "sniperHW").Exec().ErrCode {
				break
			}
			time.Sleep(time.Second)
		}

		r1 = c.SetNx("users1", "sniperHW", fields).Exec()
		assert.Nil(t, r1.ErrCode)*/

	}

	/*fmt.Println("-----------------------------compareAndSet----------------------------------")
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

		for {
			if nil == c.Kick("users1", "sniperHW").Exec().ErrCode {
				break
			}
			time.Sleep(time.Second)
		}

		r2 = c.CompareAndSet("users1", "sniperHW", "age", 11, 10).Exec()
		assert.Equal(t, r2.ErrCode, Err_record_notexist)

		time.Sleep(time.Second)

		for {
			if nil == c.Kick("users1", "sniperHW").Exec().ErrCode {
				break
			}
			time.Sleep(time.Second)
		}

		r2 = c.CompareAndSet("users1", "sniperHW", "age", 10, 11).Exec()
		assert.Equal(t, r2.ErrCode, Err_record_notexist)

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

		for {
			if nil == c.Kick("users1", "sniperHW").Exec().ErrCode {
				break
			}
			time.Sleep(time.Second)
		}

		r2 = c.CompareAndSetNx("users1", "sniperHW", "age", 11, 12, 1).Exec()
		assert.Equal(t, r2.ErrCode, Err_version_mismatch)

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
		assert.Equal(t, r1.ErrCode, Err_version_mismatch)

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
		assert.Equal(t, r1.ErrCode, Err_version_mismatch)

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
	}*/
}

func Test1Node1Store1(t *testing.T) {

	InitLogger(logger.NewZapLogger("testRaft.log", "./log", config.Log.LogLevel, config.Log.MaxLogfileSize, config.Log.MaxAge, config.Log.MaxBackups, config.Log.EnableStdout))

	//先删除所有kv文件
	os.RemoveAll("./testRaftLog")

	client.InitLogger(GetLogger())

	node := start1Node(newSqlDBBackEnd())

	c, _ := client.OpenClient(client.ClientConf{SoloService: "localhost:10018", UnikeyPlacement: GetStore})

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

	InitLogger(logger.NewZapLogger("testRaft.log", "./log", config.Log.LogLevel, config.Log.MaxLogfileSize, config.Log.MaxAge, config.Log.MaxBackups, config.Log.EnableStdout))

	//先删除所有kv文件
	os.RemoveAll("./testRaftLog")

	client.InitLogger(GetLogger())

	node := start1Node(newSqlDBBackEnd())

	c, _ := client.OpenClient(client.ClientConf{SoloService: "localhost:10018", UnikeyPlacement: GetStore})

	test(t, c)

	node.Stop()

	fmt.Println("stop ok")

}

func Test1Node1StoreSnapshot1(t *testing.T) {

	oldV := config.MaxCachePerStore

	config.MaxCachePerStore = 10000

	raft.SnapshotCount = 100
	raft.SnapshotCatchUpEntriesN = 100

	InitLogger(logger.NewZapLogger("testRaft.log", "./log", config.Log.LogLevel, config.Log.MaxLogfileSize, config.Log.MaxAge, config.Log.MaxBackups, config.Log.EnableStdout))

	//先删除所有kv文件
	os.RemoveAll("./testRaftLog")

	client.InitLogger(GetLogger())

	node := start1Node(newMockDBBackEnd())

	c, _ := client.OpenClient(client.ClientConf{SoloService: "localhost:10018", UnikeyPlacement: GetStore})

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
	for {
		if nil == c.Kick("users1", "sniperHW:99").Exec().ErrCode {
			break
		}
		time.Sleep(time.Second)
	}

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
	for {
		if nil == c.Kick("users1", "sniperHW:199").Exec().ErrCode {
			break
		}
		time.Sleep(time.Second)
	}

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

	assert.Equal(t, 299, len(node.stores[1].kv[0]))

	assert.Nil(t, node.stores[1].kv[0]["users1:sniperHW:199"])

	assert.NotNil(t, node.stores[1].kv[0]["users1:sniperHW:99"])

	node.Stop()

	config.MaxCachePerStore = oldV

}

func Test1Node1StoreSnapshot2(t *testing.T) {

	raft.SnapshotCount = 100
	raft.SnapshotCatchUpEntriesN = 100

	InitLogger(logger.NewZapLogger("testRaft.log", "./log", config.Log.LogLevel, config.Log.MaxLogfileSize, config.Log.MaxAge, config.Log.MaxBackups, config.Log.EnableStdout))

	//先删除所有kv文件
	os.RemoveAll("./testRaftLog")

	client.InitLogger(GetLogger())

	node := start1Node(newMockDBBackEnd())

	c, _ := client.OpenClient(client.ClientConf{SoloService: "localhost:10018", UnikeyPlacement: GetStore})

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
	for {
		if nil == c.Kick("users1", "sniperHW:49").Exec().ErrCode {
			break
		}
		time.Sleep(time.Second)
	}

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

}

func TestUseMockDB(t *testing.T) {
	InitLogger(logger.NewZapLogger("testRaft.log", "./log", config.Log.LogLevel, config.Log.MaxLogfileSize, config.Log.MaxAge, config.Log.MaxBackups, config.Log.EnableStdout))

	//先删除所有kv文件
	os.RemoveAll("./testRaftLog")

	client.InitLogger(GetLogger())

	node := start1Node(newMockDBBackEnd())

	c, _ := client.OpenClient(client.ClientConf{SoloService: "localhost:10018", UnikeyPlacement: GetStore})

	test(t, c)

	node.Stop()

	fmt.Println("stop ok")
}

func TestKick(t *testing.T) {
	InitLogger(logger.NewZapLogger("testRaft.log", "./log", config.Log.LogLevel, config.Log.MaxLogfileSize, config.Log.MaxAge, config.Log.MaxBackups, config.Log.EnableStdout))

	//先删除所有kv文件
	os.RemoveAll("./testRaftLog")

	client.InitLogger(GetLogger())

	node := start1Node(newMockDBBackEnd())

	c, _ := client.OpenClient(client.ClientConf{SoloService: "localhost:10018", UnikeyPlacement: GetStore})

	for j := 0; j < 10; j++ {
		for i := 0; i < 20; i++ {
			fields := map[string]interface{}{}
			fields["age"] = 12
			fields["name"] = "sniperHW"
			c.Set("users1", fmt.Sprintf("sniperHW:%d", i), fields).Exec()
		}
	}

	node.Stop()

	node = start1Node(newMockDBBackEnd())

	time.Sleep(time.Second * 5)

	node.Stop()

	fmt.Println("stop ok")
}

func TestMakeUnikeyPlacement(t *testing.T) {
	fn := sslot.MakeUnikeyPlacement([]int{1, 2, 3, 4, 5})
	fmt.Println(fn("users1:huangwei:247100"), sslot.Unikey2Slot("users1:huangwei:247100"))
}

/*
func TestUpdateMeta(t *testing.T) {
	InitLogger(logger.NewZapLogger("testRaft.log", "./log", config.Log.LogLevel, config.Log.MaxLogfileSize, config.Log.MaxAge, config.Log.MaxBackups, config.Log.EnableStdout))

	//先删除所有kv文件
	os.RemoveAll("./testRaftLog")

	client.InitLogger(GetLogger())

	node := start1Node(newMockDBBackEnd())

	def, _ := db.CreateDbDefFromCsv([]string{"users1@name:string:,age:int:,phone:string:"})

	defJson, _ := db.DbDefToJsonString(def)

	update := &sproto.NotifyUpdateMeta{
		Store:   1,
		Version: 2,
		Meta:    defJson,
	}

	conn, err := fnet.NewUdp("localhost:0", snet.Pack, snet.Unpack)
	assert.Nil(t, err)

	addr, err := net.ResolveUDPAddr("udp", "127.0.0.1:10018")

	conn.SendTo(addr, snet.MakeMessage(0, update))
	recvbuff := make([]byte, 256)
	_, r, err := conn.ReadFrom(recvbuff)

	assert.Equal(t, r.(*snet.Message).Msg.(*sproto.StoreUpdateMetaOk).Store, int32(1))

	assert.Equal(t, r.(*snet.Message).Msg.(*sproto.StoreUpdateMetaOk).Version, int64(2))

	//again

	conn.SendTo(addr, snet.MakeMessage(0, update))

	_, r, err = conn.ReadFrom(recvbuff)

	assert.Equal(t, r.(*snet.Message).Msg.(*sproto.StoreUpdateMetaOk).Store, int32(1))

	assert.Equal(t, r.(*snet.Message).Msg.(*sproto.StoreUpdateMetaOk).Version, int64(2))

	conn.Close()

	node.Stop()

}*/

func TestSlotTransferIn(t *testing.T) {
	InitLogger(logger.NewZapLogger("testRaft.log", "./log", config.Log.LogLevel, config.Log.MaxLogfileSize, config.Log.MaxAge, config.Log.MaxBackups, config.Log.EnableStdout))

	//先删除所有kv文件
	os.RemoveAll("./testRaftLog")

	client.InitLogger(GetLogger())

	node := start1Node(newMockDBBackEnd())

	store1 := node.stores[1]
	slots := store1.slots.GetOpenBits()
	store1.slots.Clear(slots[0])

	conn, err := fnet.NewUdp("localhost:0", snet.Pack, snet.Unpack)
	assert.Nil(t, err)

	addr, err := net.ResolveUDPAddr("udp", "127.0.0.1:10018")

	conn.SendTo(addr, snet.MakeMessage(0, &sproto.NotifySlotTransIn{
		Store: int32(1),
		Slot:  int32(slots[0]),
	}))

	recvbuff := make([]byte, 256)
	_, r, err := conn.ReadFrom(recvbuff)

	assert.Equal(t, r.(*snet.Message).Msg.(*sproto.SlotTransInOk).Slot, int32(slots[0]))

	//again

	conn.SendTo(addr, snet.MakeMessage(0, &sproto.NotifySlotTransIn{
		Store: int32(1),
		Slot:  int32(slots[0]),
	}))

	_, r, err = conn.ReadFrom(recvbuff)

	assert.Equal(t, r.(*snet.Message).Msg.(*sproto.SlotTransInOk).Slot, int32(slots[0]))

	conn.Close()

	node.Stop()

}

func TestSlotTransferOut(t *testing.T) {
	InitLogger(logger.NewZapLogger("testRaft.log", "./log", config.Log.LogLevel, config.Log.MaxLogfileSize, config.Log.MaxAge, config.Log.MaxBackups, config.Log.EnableStdout))

	//先删除所有kv文件
	os.RemoveAll("./testRaftLog")

	client.InitLogger(GetLogger())

	node := start1Node(newMockDBBackEnd())

	store1 := node.stores[1]
	slots := store1.slots.GetOpenBits()
	outSlot := slots[0]

	c, _ := client.OpenClient(client.ClientConf{SoloService: "localhost:10018", UnikeyPlacement: GetStore})

	for j := 0; j < 10; j++ {
		for i := 0; i < 10; i++ {
			fields := map[string]interface{}{}
			fields["age"] = 12
			fields["name"] = "sniperHW"
			c.Set("users1", fmt.Sprintf("sniperHW:%d", i), fields).Exec()
		}
	}

	fmt.Println("NotifySlotTransOut")

	for {
		conn, err := fnet.NewUdp("localhost:0", snet.Pack, snet.Unpack)
		assert.Nil(t, err)

		addr, err := net.ResolveUDPAddr("udp", "127.0.0.1:10018")

		conn.SendTo(addr, snet.MakeMessage(0, &sproto.NotifySlotTransOut{
			Store: int32(1),
			Slot:  int32(outSlot),
		}))

		respCh := make(chan *snet.Message)
		var ret *snet.Message

		go func() {
			recvbuff := make([]byte, 256)
			_, r, err := conn.ReadFrom(recvbuff)
			if nil == err {
				select {
				case respCh <- r.(*snet.Message):
				default:
				}
			}
		}()

		ticker := time.NewTicker(3 * time.Second)

		select {
		case ret = <-respCh:
		case <-ticker.C:
		}

		ticker.Stop()

		conn.Close()

		if nil != ret {
			assert.Equal(t, ret.Msg.(*sproto.SlotTransOutOk).Slot, int32(outSlot))
			break
		}

	}

	node.Stop()

}
