package flykv

//go test -covermode=count -v -coverprofile=coverage.out -run=.
//go tool cover -html=coverage.out

import (
	"fmt"
	"github.com/BurntSushi/toml"
	"github.com/sniperHW/flyfish/backend/db/sql"
	"github.com/sniperHW/flyfish/client"
	"github.com/sniperHW/flyfish/logger"
	"github.com/sniperHW/flyfish/pkg/raft"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
	"time"
)

func TestMutilNode(t *testing.T) {
	var configStr1 string = `
	
	Mode = "solo"

	DBType = "pgsql"
	
	SnapshotCurrentCount    = 1
	
	LruCheckInterval        = 100              #每隔100ms执行一次lru剔除操作

	MainQueueMaxSize        = 10000
	
	MaxCachePerStore        = 100               #每组最大key数量，超过数量将会触发key剔除
	
	SqlLoadPipeLineSize     = 200                  #sql加载管道线大小
	
	SqlLoadQueueSize        = 10000                #sql加载请求队列大小，此队列每CacheGroup一个
	
	SqlLoaderCount          = 5
	SqlUpdaterCount         = 5
	
	ProposalFlushInterval   = 100
	ReadFlushInterval       = 10 
	
	
	[SoloConfig]
	RaftUrl 				=  "%s"	
	ServiceHost             = "127.0.0.1"
	ServicePort             = %d
	RaftCluster             = "1@http://127.0.0.1:12377,2@http://127.0.0.1:12378,3@http://127.0.0.1:12379"
	Stores                  = [1]
	
						  
	[DBConfig]

	
	
	DbHost          = "%s"
	DbPort          = %d
	DbUser			= "%s"
	DbPassword      = "%s"
	DataDB          = "%s"
	ConfDB          = "%s"
	
	
	[Log]
	MaxLogfileSize  = 104857600 # 100mb
	LogDir          = "log"
	LogPrefix       = "flyfish"
	LogLevel        = "info"
	EnableLogStdout = false	
	`

	DefaultSnapshotCount := raft.DefaultSnapshotCount
	SnapshotCatchUpEntriesN := raft.SnapshotCatchUpEntriesN

	raft.DefaultSnapshotCount = 100
	raft.SnapshotCatchUpEntriesN = 100

	InitLogger(logger.NewZapLogger("testRaft.log", "./log", "info", 100, 14, true))

	//先删除所有kv文件
	os.RemoveAll("./log/kvnode-1-1")
	os.RemoveAll("./log/kvnode-1-1-snap")
	os.RemoveAll("./log/kvnode-2-1")
	os.RemoveAll("./log/kvnode-2-1-snap")
	os.RemoveAll("./log/kvnode-3-1")
	os.RemoveAll("./log/kvnode-3-1-snap")
	os.RemoveAll("./log/kvnode-4-1")
	os.RemoveAll("./log/kvnode-4-1-snap")

	dbConf := &dbconf{}
	if _, err := toml.DecodeFile("test_dbconf.toml", dbConf); nil != err {
		panic(err)
	}

	conf1, _ := LoadConfigStr(fmt.Sprintf(configStr1, "http://127.0.0.1:12377", 10018, "localhost", 5432, dbConf.PgDB, dbConf.PgDB))
	conf2, _ := LoadConfigStr(fmt.Sprintf(configStr1, "http://127.0.0.1:12378", 10017, "localhost", 5432, dbConf.PgDB, dbConf.PgDB))
	conf3, _ := LoadConfigStr(fmt.Sprintf(configStr1, "http://127.0.0.1:12379", 10016, "localhost", 5432, dbConf.PgDB, dbConf.PgDB))

	client.InitLogger(GetLogger())

	node1 := NewKvNode(1, conf1, dbMeta, sql.CreateDbMeta, newMockDBBackEnd())

	if err := node1.Start(); nil != err {
		panic(err)
	}

	node2 := NewKvNode(2, conf2, dbMeta, sql.CreateDbMeta, newMockDBBackEnd())

	if err := node2.Start(); nil != err {
		panic(err)
	}

	node3 := NewKvNode(3, conf3, dbMeta, sql.CreateDbMeta, newMockDBBackEnd())

	if err := node3.Start(); nil != err {
		panic(err)
	}

	//等待有一个节点成为leader
	var leader *kvnode
	ok := make(chan struct{})

	go func() {
		for {
			select {
			case <-ok:
				return
			default:
			}

			if node1.stores[1].isLeader() {
				leader = node1
				close(ok)
				return
			}
		}
	}()

	go func() {
		for {
			select {
			case <-ok:
				return
			default:
			}

			if node2.stores[1].isLeader() {
				leader = node2
				close(ok)
				return
			}
		}
	}()

	go func() {
		for {
			select {
			case <-ok:
				return
			default:
			}

			if node3.stores[1].isLeader() {
				leader = node3
				close(ok)
				return
			}
		}
	}()

	<-ok

	c, _ := client.OpenClient(client.ClientConf{SoloService: fmt.Sprintf("localhost:%d", leader.config.SoloConfig.ServicePort), UnikeyPlacement: GetStore})

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

	var configStr2 string = `
	
	Mode = "solo"

	DBType = "pgsql"
	
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
	RaftUrl                 =  "%s"	
	ServiceHost             = "127.0.0.1"
	ServicePort             = %d
	RaftCluster             = "1@http://127.0.0.1:12377,2@http://127.0.0.1:12378,3@http://127.0.0.1:12379,4@http://127.0.0.1:22381"
	Stores                  = [1]
	
						  
	[DBConfig]

	
	
	Host          = "%s"
	Port          = %d
	User		  = "%s"
	Password      = "%s"
	DataDB        = "%s"
	ConfDB        = "%s"
	
	
	[Log]
	MaxLogfileSize  = 104857600 # 100mb
	LogDir          = "log"
	LogPrefix       = "flyfish"
	LogLevel        = "info"
	EnableLogStdout = false	
	`

	conf4, _ := LoadConfigStr(fmt.Sprintf(configStr2, "http://127.0.0.1:22381", 10019, "localhost", 5432, dbConf.PgUser, dbConf.PgPwd, dbConf.PgDB, dbConf.PgDB))

	node4 := NewKvNode(4, conf4, dbMeta, sql.CreateDbMeta, newMockDBBackEnd())

	if err := node4.Start(); nil != err {
		panic(err)
	}

	newNodeID := uint64((4 << 16) + 1)
	leader.stores[1].addNode(newNodeID, "http://127.0.0.1:22381")

	time.Sleep(time.Second * 5)

	assert.Equal(t, 299, len(node1.stores[1].keyvals[0].kv))
	assert.Equal(t, 299, len(node2.stores[1].keyvals[0].kv))
	assert.Equal(t, 299, len(node3.stores[1].keyvals[0].kv))
	assert.Equal(t, 299, len(node4.stores[1].keyvals[0].kv))

	node1.Stop()
	fmt.Println("stop1")
	node2.Stop()
	fmt.Println("stop2")
	node3.Stop()
	fmt.Println("stop3")
	node4.Stop()
	fmt.Println("stop4")

	raft.DefaultSnapshotCount = DefaultSnapshotCount
	raft.SnapshotCatchUpEntriesN = SnapshotCatchUpEntriesN
}
