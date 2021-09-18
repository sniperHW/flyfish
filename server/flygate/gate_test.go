package flygate

//go test -covermode=count -v -coverprofile=coverage.out -run=.
//go tool cover -html=coverage.out

import (
	"fmt"
	"github.com/sniperHW/flyfish/backend/db"
	"github.com/sniperHW/flyfish/client"
	"github.com/sniperHW/flyfish/errcode"
	"github.com/sniperHW/flyfish/logger"
	"github.com/sniperHW/flyfish/server/clusterconf"
	"github.com/sniperHW/flyfish/server/mock/kvnode"
	sslot "github.com/sniperHW/flyfish/server/slot"
	"github.com/stretchr/testify/assert"
	"testing"
)

var (
	Err_version_mismatch errcode.Error = errcode.New(errcode.Errcode_version_mismatch, "")
	Err_record_exist     errcode.Error = errcode.New(errcode.Errcode_record_exist, "")
	Err_record_notexist  errcode.Error = errcode.New(errcode.Errcode_record_notexist, "")
	Err_record_unchange  errcode.Error = errcode.New(errcode.Errcode_record_unchange, "")
	Err_cas_not_equal    errcode.Error = errcode.New(errcode.Errcode_cas_not_equal, "")
	Err_timeout          errcode.Error = errcode.New(errcode.Errcode_timeout, "")
)

var metaStr string = `
{"TableDefs":[{"Name":"users1","Fields":[{"Name":"name","Type":"string"},{"Name":"age","Type":"int","DefautValue":"0"},{"Name":"phone","Type":"string"}]}]}
`

var configStr string = `
	ServiceHost = "localhost"
	ServicePort = 8110
	ConsolePort = 8110

	MaxNodePendingMsg  = 4096
	MaxStorePendingMsg = 1024
	MaxPendingMsg = 10000

	[ClusterConfig]
		ClusterID               = 1
		DbHost                  = "localhost"
		DbPort                  = 5432
		DbUser			        = "sniper"
		DbPassword              = "123456"
		DbDataBase              = "test"

	[Log]
		MaxLogfileSize  = 104857600 # 100mb
		LogDir          = "log"
		LogPrefix       = "gate"
		LogLevel        = "info"
		EnableLogStdout = false		

`

func test(t *testing.T, c *client.Client) {

	{

		c.Del("users1", "sniperHW").Exec()

		fields := map[string]interface{}{}
		fields["age"] = 12
		fields["name"] = "sniperHW"

		r1 := c.Set("users1", "sniperHW", fields).Exec()
		assert.Nil(t, r1.ErrCode)
		fmt.Println("version-----------", r1.Version)

		c.Del("users1", "sniperHW").Exec()

		r2 := c.Set("users1", "sniperHW", fields).Exec()
		assert.Nil(t, r2.ErrCode)
		fmt.Println(r1.Version, r2.Version)

		r3 := c.Set("users1", "sniperHW", fields, r1.Version).Exec()
		assert.Equal(t, Err_version_mismatch, r3.ErrCode)

		fmt.Println(r1.Version, r3.Version)

	}

	{
		fields := map[string]interface{}{}
		fields["age"] = 12
		fields["name"] = "sniperHW"

		//get/set/setnx/del
		{
			r := c.Set("users1", "sniperHW", fields).Exec()
			assert.Nil(t, r.ErrCode)
		}

		{
			r := c.GetAll("users1", "sniperHW").Exec()
			assert.Nil(t, r.ErrCode)
			assert.Equal(t, "sniperHW", r.Fields["name"].GetString())
			assert.Equal(t, int64(12), r.Fields["age"].GetInt())
		}

		{
			r := c.Del("users1", "sniperHW").Exec()
			assert.Nil(t, r.ErrCode)
		}

		{
			r := c.Del("users1", "sniperHW").Exec()
			assert.Equal(t, Err_record_notexist, r.ErrCode)
		}

		{
			r := c.GetAll("users1", "sniperHW").Exec()
			assert.Equal(t, Err_record_notexist, r.ErrCode)
		}

		{
			fields := map[string]interface{}{}
			fields["ages"] = 12
			r := c.Set("test", "sniperHW", fields).Exec()
			assert.Equal(t, errcode.Errcode_error, r.ErrCode.Code)
		}

		{

			fields := map[string]interface{}{}
			fields["ages"] = 12
			r := c.Set("users1", "sniperHW", fields).Exec()
			assert.Equal(t, errcode.Errcode_error, r.ErrCode.Code)
		}

		{
			r := c.Set("users1", "sniperHW", fields).Exec()
			assert.Nil(t, r.ErrCode)
		}

		{

			r0 := c.GetAll("users1", "sniperHW").Exec()
			assert.Nil(t, r0.ErrCode)

			r := c.Set("users1", "sniperHW", fields).Exec()
			assert.Nil(t, r.ErrCode)
			assert.Equal(t, r0.Version+1, r.Version)
		}

		{
			r := c.GetAllWithVersion("test", "sniperHW", int64(2)).Exec()
			assert.Equal(t, errcode.Errcode_error, r.ErrCode.Code)
		}

		{

			r0 := c.GetAll("users1", "sniperHW").Exec()
			assert.Nil(t, r0.ErrCode)

			r := c.GetAllWithVersion("users1", "sniperHW", r0.Version).Exec()
			assert.Equal(t, Err_record_unchange, r.ErrCode)
		}

		{
			r := c.Set("users1", "sniperHW", fields, int64(1)).Exec()
			assert.Equal(t, Err_version_mismatch, r.ErrCode)
		}

		{
			r0 := c.GetAll("users1", "sniperHW").Exec()
			assert.Nil(t, r0.ErrCode)

			r := c.Set("users1", "sniperHW", fields, r0.Version).Exec()
			assert.Nil(t, r.ErrCode)
			assert.Equal(t, r0.Version+1, r.Version)
		}

		{
			fields := map[string]interface{}{}
			fields["ages"] = 12
			r := c.SetNx("test", "sniperHW", fields).Exec()
			assert.Equal(t, errcode.Errcode_error, r.ErrCode.Code)
		}

		{

			fields := map[string]interface{}{}
			fields["ages"] = 12
			r := c.SetNx("users1", "sniperHW", fields).Exec()
			assert.Equal(t, errcode.Errcode_error, r.ErrCode.Code)
		}

		{
			r := c.SetNx("users1", "sniperHW", fields).Exec()
			assert.Equal(t, Err_record_exist, r.ErrCode)
			assert.Equal(t, "sniperHW", r.Fields["name"].GetString())
		}

		{
			r := c.Del("users1", "sniperHW", int64(1)).Exec()
			assert.Equal(t, Err_version_mismatch, r.ErrCode)
		}

		{
			r0 := c.GetAll("users1", "sniperHW").Exec()
			assert.Nil(t, r0.ErrCode)

			r := c.Del("users1", "sniperHW", r0.Version).Exec()
			assert.Nil(t, r.ErrCode)
		}

		{
			r := c.SetNx("users1", "sniperHW", fields).Exec()
			assert.Nil(t, r.ErrCode)
		}

		//incr/decr

		{
			r := c.IncrBy("users1", "sniperHW", "age", 1).Exec()
			assert.Nil(t, r.ErrCode)
			assert.Equal(t, int64(13), r.Fields["age"].GetInt())
		}

		{
			r := c.DecrBy("users1", "sniperHW", "age", 1).Exec()
			assert.Nil(t, r.ErrCode)
			assert.Equal(t, int64(12), r.Fields["age"].GetInt())
		}

		{
			r := c.Del("users1", "sniperHW").Exec()
			assert.Nil(t, r.ErrCode)
		}

		{
			r := c.IncrBy("users1", "sniperHW", "age", 1).Exec()
			assert.Nil(t, r.ErrCode)
			assert.Equal(t, int64(1), r.Fields["age"].GetInt())
		}

		{
			r := c.IncrBy("test", "sniperHW", "age", 1, 10).Exec()
			assert.Equal(t, errcode.Errcode_error, r.ErrCode.Code)
		}

		{
			r := c.IncrBy("users1", "sniperHW", "age1", 1, 10).Exec()
			assert.Equal(t, errcode.Errcode_error, r.ErrCode.Code)
		}

		{
			r := c.IncrBy("users1", "sniperHW", "age", 1, 10).Exec()
			assert.Equal(t, Err_version_mismatch, r.ErrCode)
		}

		{
			r := c.Del("users1", "sniperHW").Exec()
			assert.Nil(t, r.ErrCode)
		}

		{
			r := c.DecrBy("test", "sniperHW", "age", 1, 10).Exec()
			assert.Equal(t, errcode.Errcode_error, r.ErrCode.Code)
		}

		{
			r := c.DecrBy("users1", "sniperHW", "age1", 1, 10).Exec()
			assert.Equal(t, errcode.Errcode_error, r.ErrCode.Code)
		}

		{
			r := c.DecrBy("users1", "sniperHW", "age", 1).Exec()
			assert.Nil(t, r.ErrCode)
			assert.Equal(t, int64(-1), r.Fields["age"].GetInt())
		}

		{
			r := c.DecrBy("users1", "sniperHW", "age", 1, 10).Exec()
			assert.Equal(t, Err_version_mismatch, r.ErrCode)
		}

		//compare
		{
			r := c.CompareAndSet("users1", "sniperHW", "age", -1, 100).Exec()
			assert.Nil(t, r.ErrCode)
		}

		{
			r := c.CompareAndSet("users1", "sniperHW", "age", -1, 100, 1000).Exec()
			assert.Equal(t, Err_version_mismatch, r.ErrCode)
		}

		{
			r := c.CompareAndSet("test", "sniperHW", "age", 1, 10).Exec()
			assert.Equal(t, errcode.Errcode_error, r.ErrCode.Code)
		}

		{
			r := c.CompareAndSet("users1", "sniperHW", "age1", 1, 10).Exec()
			assert.Equal(t, errcode.Errcode_error, r.ErrCode.Code)
		}

		{
			r := c.Del("users1", "sniperHW").Exec()
			assert.Nil(t, r.ErrCode)
		}

		{
			r := c.CompareAndSet("users1", "sniperHW", "age", -1, 100).Exec()
			assert.Equal(t, Err_record_notexist, r.ErrCode)
		}

		{
			r := c.CompareAndSetNx("users1", "sniperHW", "age", -1, 100).Exec()
			assert.Nil(t, r.ErrCode)
		}

		{
			r := c.CompareAndSetNx("users1", "sniperHW", "age", -1, 100, 1000).Exec()
			assert.Equal(t, Err_version_mismatch, r.ErrCode)
		}

		{
			r := c.CompareAndSetNx("test", "sniperHW", "age", 1, 10).Exec()
			assert.Equal(t, errcode.Errcode_error, r.ErrCode.Code)
		}

		{
			r := c.CompareAndSetNx("users1", "sniperHW", "age1", 1, 10).Exec()
			assert.Equal(t, errcode.Errcode_error, r.ErrCode.Code)
		}

		{
			r := c.CompareAndSet("users1", "sniperHW", "age", 101, 1).Exec()
			assert.Equal(t, Err_cas_not_equal, r.ErrCode)
		}

		{
			r := c.CompareAndSetNx("users1", "sniperHW", "age", 101, 1).Exec()
			assert.Equal(t, Err_cas_not_equal, r.ErrCode)
		}

		{
			r := c.CompareAndSetNx("users1", "sniperHW", "age", 100, 1).Exec()
			assert.Nil(t, r.ErrCode)
		}

		{
			r := c.Kick("users1", "sniperHW").Exec()
			assert.Nil(t, r.ErrCode)
		}
	}
}

func Test1(t *testing.T) {

	sslot.SlotCount = 128

	confJson := clusterconf.KvConfigJson{}

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

	_, version, _ := clusterconf.LoadConfigJsonFromDB(1, "pgsql", "localhost", 5432, "test", "sniper", "123456")

	oldVersion := version
	newVersion := version + 1

	err := clusterconf.StoreConfigJsonToDB(1, oldVersion, newVersion, "pgsql", "localhost", 5432, "test", "sniper", "123456", &confJson)

	assert.Nil(t, err)

	def, _ := db.CreateDbDefFromJsonString([]byte(metaStr))
	n1 := kvnode.New()
	n2 := kvnode.New()
	n3 := kvnode.New()

	assert.Nil(t, n1.Start(true, "localhost:8021", "localhost:8031", def))
	assert.Nil(t, n2.Start(false, "localhost:8022", "localhost:8032", def))
	assert.Nil(t, n3.Start(false, "localhost:8023", "localhost:8033", def))

	config, err := LoadConfigStr(configStr)

	assert.Nil(t, err)

	l := logger.NewZapLogger("testGate.log", "./log", config.Log.LogLevel, 100, 14, true)

	InitLogger(l)

	client.InitLogger(l)

	g := NewGate(config)

	assert.Nil(t, g.Start())

	c := client.OpenClient("localhost:8110")
	test(t, c)

	g.config.MaxNodePendingMsg = 0

	r := c.Del("users1", "sniperHW").Exec()

	assert.Equal(t, errcode.Errcode_gate_busy, errcode.GetCode(r.ErrCode))

	g.Stop()
	n1.Stop()
	n2.Stop()
	n3.Stop()

}
