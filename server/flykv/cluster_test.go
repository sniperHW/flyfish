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
	"github.com/sniperHW/flyfish/server/clusterconf"
	sslot "github.com/sniperHW/flyfish/server/slot"
	//"github.com/stretchr/testify/assert"
	"os"
	"testing"
	//"time"
)

func TestCluster1(t *testing.T) {
	var configStr1 string = `

		Mode = "cluster"

		DBType         = "pgsql"

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


		[ClusterConfig]
		ClusterID               = 1
		DBHost                  = "localhost"
		DBPort                  = 5432
		DBUser			        = "sniper"
		DBPassword              = "123456"
		ConfDB                  = "test"


		[DBConfig]

		Host            = "%s"
		Port            = %d
		User		    = "%s"
		Password        = "%s"
		DataDB          = "%s"
		ConfDBB         = "%s"


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

	sslot.SlotCount = 128

	confJson := clusterconf.KvConfigJson{}

	clusterconf.StorePerNode = 1

	confJson.NodeInfo = append(confJson.NodeInfo, clusterconf.Node{
		ID:          1,
		HostIP:      "localhost",
		RaftPort:    8011,
		ServicePort: 8021,
		ConsolePort: 8031,
	})

	confJson.NodeInfo = append(confJson.NodeInfo, clusterconf.Node{
		ID:          2,
		HostIP:      "localhost",
		RaftPort:    8012,
		ServicePort: 8022,
		ConsolePort: 8032,
	})

	confJson.NodeInfo = append(confJson.NodeInfo, clusterconf.Node{
		ID:          3,
		HostIP:      "localhost",
		RaftPort:    8013,
		ServicePort: 8023,
		ConsolePort: 8033,
	})

	confJson.Shard = append(confJson.Shard, clusterconf.RaftGroupJson{
		Nodes: []int{1, 2, 3},
	})

	clusterconf.StoreConfigJsonToDB(1, "pgsql", "localhost", 5432, "test", "sniper", "123456", &confJson)

	InitLogger(logger.NewZapLogger("testRaft.log", "./log", "info", 100, 14, true))

	//先删除所有kv文件
	os.RemoveAll("./log/kvnode-1-1")
	os.RemoveAll("./log/kvnode-1-1-snap")
	os.RemoveAll("./log/kvnode-2-1")
	os.RemoveAll("./log/kvnode-2-1-snap")
	os.RemoveAll("./log/kvnode-3-1")
	os.RemoveAll("./log/kvnode-3-1-snap")

	dbConf := &dbconf{}
	if _, err := toml.DecodeFile("test_dbconf.toml", dbConf); nil != err {
		panic(err)
	}

	conf1, _ := LoadConfigStr(fmt.Sprintf(configStr1, "localhost", 5432, dbConf.PgUser, dbConf.PgPwd, dbConf.PgDB, dbConf.PgDB))

	client.InitLogger(GetLogger())

	node1 := NewKvNode(1, conf1, dbMeta, sql.CreateDbMeta, newMockDBBackEnd())

	if err := node1.Start(); nil != err {
		panic(err)
	}

	node2 := NewKvNode(2, conf1, dbMeta, sql.CreateDbMeta, newMockDBBackEnd())

	if err := node2.Start(); nil != err {
		panic(err)
	}

	node3 := NewKvNode(3, conf1, dbMeta, sql.CreateDbMeta, newMockDBBackEnd())

	if err := node3.Start(); nil != err {
		panic(err)
	}

	//等待有一个节点成为leader
	//var leader *kvnode
	ok := make(chan struct{})

	go func() {
		for {
			select {
			case <-ok:
				return
			default:
			}

			if node1.stores[1].isLeader() {
				//leader = node1
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
				//leader = node2
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
				//leader = node3
				close(ok)
				return
			}
		}
	}()

	<-ok

	node1.Stop()
	fmt.Println("stop1")
	node2.Stop()
	fmt.Println("stop2")
	node3.Stop()
	fmt.Println("stop3")

	raft.DefaultSnapshotCount = DefaultSnapshotCount
	raft.SnapshotCatchUpEntriesN = SnapshotCatchUpEntriesN
}
