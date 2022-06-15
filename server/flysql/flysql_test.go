package flysql

//go test -race -covermode=atomic -v -coverprofile=../coverage.out -run=.
//go tool cover -html=../coverage.out

import (
	"fmt"
	"github.com/BurntSushi/toml"
	"github.com/sniperHW/flyfish/client"
	"github.com/sniperHW/flyfish/db/sql"
	"github.com/sniperHW/flyfish/errcode"
	"github.com/sniperHW/flyfish/logger"
	"github.com/stretchr/testify/assert"
	"net/http"
	_ "net/http/pprof"
	"strings"
	"sync"
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

var config *Config
var dbConf *dbconf

var clearUsers1 func()

func init() {

	go func() {
		http.ListenAndServe("localhost:6060", nil)
	}()

	var err error

	dbConf = &dbconf{}
	if _, err = toml.DecodeFile("test/test_dbconf.toml", dbConf); nil != err {
		panic(err)
	}

	config, err = LoadConfigStr(fmt.Sprintf(configStr, dbConf.DBType, dbConf.Host, dbConf.Port, dbConf.User, dbConf.Pwd, dbConf.DB))

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

func test(t *testing.T, c *client.Client) {
	{
		dbc, _ := sql.SqlOpen(dbConf.DBType, dbConf.Host, dbConf.Port, dbConf.DB, dbConf.User, dbConf.Pwd)

		dbc.Exec("delete from users1_0 where __key__ = 'sniperHW';")

		r1 := c.Del("users1", "sniperHW").Exec()
		assert.Nil(t, r1.ErrCode)

		fields := map[string]interface{}{}
		fields["age"] = 12
		fields["name"] = "sniperHW"
		fields["phone"] = []byte(strings.Repeat("a", 4096))

		rr := c.Set("users1", "sniperHW", fields).Exec()
		assert.Nil(t, rr.ErrCode)

		rr = c.Set("users1", "sniperHW", fields, 0).Exec()

		assert.Equal(t, errcode.Errcode_version_mismatch, rr.ErrCode.Code)

		r := c.GetAll("users1", "sniperHW").Exec()

		assert.Equal(t, string(r.Fields["phone"].GetBlob()), strings.Repeat("a", 4096))

		r2 := c.Del("users1", "sniperHW").Exec()
		assert.Nil(t, r2.ErrCode)

		r = c.GetAll("users1", "sniperHW").Exec()
		assert.Equal(t, errcode.Errcode_record_notexist, r.ErrCode.Code)

		delete(fields, "age")

		c.Set("users1", "sniperHW", fields).Exec()

		r = c.GetAll("users1", "sniperHW").Exec()
		assert.Nil(t, r.ErrCode)

		assert.Equal(t, r.Fields["age"].GetInt(), int64(0))
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

		r = c.GetAllWithVersion("users1", "sniperHW", *r.Version).Exec()
		assert.Equal(t, r.ErrCode.Code, errcode.Errcode_record_unchange)

		r1 := c.Del("users1", "sniperHW").Exec()
		assert.Nil(t, r1.ErrCode)

		r = c.GetAll("users1", "sniperHW").Exec()
		assert.Equal(t, r.ErrCode.Code, errcode.Errcode_record_notexist)

		r3 := c.Del("users1", "sniperHW").Exec()
		assert.Nil(t, r3.ErrCode)
	}

	fmt.Println("-----------------------------setNx----------------------------------")

	{
		r1 := c.SetNx("users1", "sniperHW", fields).Exec()
		assert.Nil(t, r1.ErrCode)

		r1 = c.SetNx("users1", "sniperHW", fields).Exec()
		assert.Equal(t, r1.ErrCode.Code, errcode.Errcode_record_exist)
	}

	fmt.Println("-----------------------------compareAndSet----------------------------------")
	{
		r1 := c.Get("users1", "sniperHW", "age").Exec()
		assert.Nil(t, r1.ErrCode)

		r2 := c.CompareAndSet("users1", "sniperHW", "age", r1.Fields["age"].GetValue(), 10).Exec()
		assert.Nil(t, r2.ErrCode)

		r2 = c.CompareAndSet("users1", "sniperHW", "age", 11, 10).Exec()
		assert.Equal(t, r2.ErrCode.Code, errcode.Errcode_cas_not_equal)

		r2 = c.CompareAndSet("users1", "sniperHW", "age", 10, 11).Exec()
		assert.Nil(t, r2.ErrCode)

		r3 := c.Del("users1", "sniperHW").Exec()
		assert.Nil(t, r3.ErrCode)

		r2 = c.CompareAndSet("users1", "sniperHW", "age", 11, 10).Exec()
		assert.Equal(t, r2.ErrCode.Code, errcode.Errcode_record_notexist)

	}

	fmt.Println("-----------------------------compareAndSetNx----------------------------------")

	{
		r2 := c.CompareAndSetNx("users1", "sniperHW", "age", 1, 20).Exec()
		assert.Nil(t, r2.ErrCode)

		r2 = c.CompareAndSetNx("users1", "sniperHW1", "age", 1, 20).Exec()
		assert.Nil(t, r2.ErrCode)

		r2 = c.CompareAndSet("users1", "sniperHW", "age", 11, 10).Exec()
		assert.Equal(t, r2.ErrCode.Code, errcode.Errcode_cas_not_equal)
	}

	fmt.Println("-----------------------------incr----------------------------------")

	{
		r1 := c.IncrBy("users1", "sniperHW", "age", 2).Exec()
		assert.Nil(t, r1.ErrCode)
		assert.Equal(t, r1.Fields["age"].GetInt(), int64(22))

		r2 := c.Del("users1", "sniperHW").Exec()
		assert.Nil(t, r2.ErrCode)

		r1 = c.IncrBy("users1", "sniperHW", "age", 2).Exec()
		assert.Nil(t, r1.ErrCode)
		assert.Equal(t, r1.Fields["age"].GetInt(), int64(2))

		r1 = c.IncrBy("users1", "sniperHW", "age", -2).Exec()
		assert.Nil(t, r1.ErrCode)
		assert.Equal(t, r1.Fields["age"].GetInt(), int64(0))
	}

}

func TestFlySql(t *testing.T) {

	InitLogger(logger.NewZapLogger("testRaft.log", "./log", config.Log.LogLevel, config.Log.MaxLogfileSize, config.Log.MaxAge, config.Log.MaxBackups, config.Log.EnableStdout))

	clearUsers1()

	client.InitLogger(GetLogger())

	flysql, err := NewFlysql("localhost:8110", config)

	if nil != err {
		panic(err)
	}

	time.Sleep(time.Second)

	c, _ := client.OpenClient(client.ClientConf{SoloService: "localhost:8110"})

	test(t, c)

	flysql.Stop()

}
