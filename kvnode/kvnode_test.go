package kvnode

//go test -covermode=count -v -coverprofile=coverage.out -run=.
//go tool cover -html=coverage.out

import (
	"fmt"
	"github.com/BurntSushi/toml"
	"github.com/sniperHW/flyfish/client"
	"github.com/sniperHW/flyfish/conf"
	"github.com/sniperHW/flyfish/errcode"
	"github.com/stretchr/testify/assert"
	"go.etcd.io/etcd/raft/raftpb"
	"os"
	"strings"
	"sync/atomic"
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
CacheGroupSize          = 1                  #cache分组数量，每一个cache组单独管理，以降低处理冲突

MaxCachePerGroupSize    = 10               #每组最大key数量，超过数量将会触发key剔除

SqlLoadPipeLineSize     = 200                  #sql加载管道线大小

SqlLoadQueueSize        = 10000                #sql加载请求队列大小，此队列每CacheGroup一个

SqlLoaderCount          = 5
SqlUpdaterCount         = 5

StrInitCap              = 1048576 # 1mb

ServiceHost             = "127.0.0.1"

ServicePort             = %d

ReplyBusyOnQueueFull    = false                #当处理队列满时是否用busy响应，如果填false,直接丢弃请求，让客户端超时 

Compress                = true

BatchByteSize           = 148576
BatchCount              = 200 
ProposalFlushInterval   = 100
ReadFlushInterval       = 10                   	 


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
LogDir          = "log1"
LogPrefix       = "flyfish"
LogLevel        = "debug"
EnableLogStdout = false	
`

func test1(t *testing.T, c *client.Client) {

	{

		c.Del("users1", "sniperHW").Exec()

		fields := map[string]interface{}{}
		fields["age"] = 12
		fields["name"] = "sniperHW"

		r1 := c.Set("users1", "sniperHW", fields).Exec()
		assert.Equal(t, errcode.ERR_OK, r1.ErrCode)
		fmt.Println("version-----------", r1.Version)

		c.Del("users1", "sniperHW").Exec()

		r2 := c.Set("users1", "sniperHW", fields).Exec()
		assert.Equal(t, errcode.ERR_OK, r2.ErrCode)
		fmt.Println(r1.Version, r2.Version)

		r3 := c.Set("users1", "sniperHW", fields, r1.Version).Exec()
		assert.Equal(t, errcode.ERR_VERSION_MISMATCH, r3.ErrCode)
		fmt.Println(r1.Version, r3.Version)

	}

	{
		fields := map[string]interface{}{}
		fields["age"] = 12
		fields["name"] = "sniperHW"

		//get/set/setnx/del
		{
			r := c.Set("users1", "sniperHW", fields).Exec()
			assert.Equal(t, errcode.ERR_OK, r.ErrCode)
		}

		{
			r := c.GetAll("users1", "sniperHW").Exec()
			assert.Equal(t, errcode.ERR_OK, r.ErrCode)
			assert.Equal(t, "sniperHW", r.Fields["name"].GetString())
			assert.Equal(t, int64(12), r.Fields["age"].GetInt())
		}

		{
			r := c.Get("users1", "sniperHW", "age").Exec()
			assert.Equal(t, errcode.ERR_OK, r.ErrCode)
			assert.Equal(t, int64(12), r.Fields["age"].GetInt())
		}

		{
			r := c.Del("users1", "sniperHW").Exec()
			assert.Equal(t, errcode.ERR_OK, r.ErrCode)
		}

		{
			r := c.Del("users1", "sniperHW").Exec()
			fmt.Println(r.ErrCode)
			assert.Equal(t, errcode.ERR_RECORD_NOTEXIST, r.ErrCode)
		}

		{
			r := c.GetAll("users1", "sniperHW").Exec()
			assert.Equal(t, errcode.ERR_RECORD_NOTEXIST, r.ErrCode)
		}

		{
			fields := map[string]interface{}{}
			fields["ages"] = 12
			r := c.Set("test", "sniperHW", fields).Exec()
			assert.Equal(t, errcode.ERR_INVAILD_TABLE, r.ErrCode)
		}

		{

			fields := map[string]interface{}{}
			fields["ages"] = 12
			r := c.Set("users1", "sniperHW", fields).Exec()
			assert.Equal(t, errcode.ERR_INVAILD_FIELD, r.ErrCode)
		}

		{
			r := c.Set("users1", "sniperHW", fields).Exec()
			assert.Equal(t, errcode.ERR_OK, r.ErrCode)
		}

		{

			r0 := c.GetAll("users1", "sniperHW").Exec()
			assert.Equal(t, errcode.ERR_OK, r0.ErrCode)

			r := c.Set("users1", "sniperHW", fields).Exec()
			assert.Equal(t, errcode.ERR_OK, r.ErrCode)
			assert.Equal(t, r0.Version+1, r.Version)
		}

		{
			r := c.GetAllWithVersion("test", "sniperHW", int64(2)).Exec()
			assert.Equal(t, errcode.ERR_INVAILD_TABLE, r.ErrCode)
		}

		{

			r0 := c.GetAll("users1", "sniperHW").Exec()
			assert.Equal(t, errcode.ERR_OK, r0.ErrCode)

			r := c.GetAllWithVersion("users1", "sniperHW", r0.Version).Exec()
			assert.Equal(t, errcode.ERR_RECORD_UNCHANGE, r.ErrCode)
		}

		{
			r := c.Set("users1", "sniperHW", fields, int64(1)).Exec()
			assert.Equal(t, errcode.ERR_VERSION_MISMATCH, r.ErrCode)
		}

		{
			r0 := c.GetAll("users1", "sniperHW").Exec()
			assert.Equal(t, errcode.ERR_OK, r0.ErrCode)

			r := c.Set("users1", "sniperHW", fields, r0.Version).Exec()
			assert.Equal(t, errcode.ERR_OK, r.ErrCode)
			assert.Equal(t, r0.Version+1, r.Version)
		}

		{
			fields := map[string]interface{}{}
			fields["ages"] = 12
			r := c.SetNx("test", "sniperHW", fields).Exec()
			assert.Equal(t, errcode.ERR_INVAILD_TABLE, r.ErrCode)
		}

		{

			fields := map[string]interface{}{}
			fields["ages"] = 12
			r := c.SetNx("users1", "sniperHW", fields).Exec()
			assert.Equal(t, errcode.ERR_INVAILD_FIELD, r.ErrCode)
		}

		{
			r := c.SetNx("users1", "sniperHW", fields).Exec()
			assert.Equal(t, errcode.ERR_RECORD_EXIST, r.ErrCode)
			assert.Equal(t, "sniperHW", r.Fields["name"].GetString())
		}

		{
			r := c.Del("users1", "sniperHW", int64(1)).Exec()
			assert.Equal(t, errcode.ERR_VERSION_MISMATCH, r.ErrCode)
		}

		{
			r0 := c.GetAll("users1", "sniperHW").Exec()
			assert.Equal(t, errcode.ERR_OK, r0.ErrCode)

			r := c.Del("users1", "sniperHW", r0.Version).Exec()
			assert.Equal(t, errcode.ERR_OK, r.ErrCode)
		}

		{
			r := c.SetNx("users1", "sniperHW", fields).Exec()
			assert.Equal(t, errcode.ERR_OK, r.ErrCode)
		}

		//incr/decr

		{
			r := c.IncrBy("users1", "sniperHW", "age", 1).Exec()
			assert.Equal(t, errcode.ERR_OK, r.ErrCode)
			assert.Equal(t, int64(13), r.Fields["age"].GetInt())
		}

		{
			r := c.DecrBy("users1", "sniperHW", "age", 1).Exec()
			assert.Equal(t, errcode.ERR_OK, r.ErrCode)
			assert.Equal(t, int64(12), r.Fields["age"].GetInt())
		}

		{
			r := c.Del("users1", "sniperHW").Exec()
			assert.Equal(t, errcode.ERR_OK, r.ErrCode)
		}

		{
			r := c.IncrBy("users1", "sniperHW", "age", 1).Exec()
			assert.Equal(t, errcode.ERR_OK, r.ErrCode)
			assert.Equal(t, int64(1), r.Fields["age"].GetInt())
		}

		{
			r := c.IncrBy("test", "sniperHW", "age", 1, 10).Exec()
			assert.Equal(t, errcode.ERR_INVAILD_TABLE, r.ErrCode)
		}

		{
			r := c.IncrBy("users1", "sniperHW", "age1", 1, 10).Exec()
			assert.Equal(t, errcode.ERR_INVAILD_FIELD, r.ErrCode)
		}

		{
			r := c.IncrBy("users1", "sniperHW", "age", 1, 10).Exec()
			assert.Equal(t, errcode.ERR_VERSION_MISMATCH, r.ErrCode)
		}

		{
			r := c.Del("users1", "sniperHW").Exec()
			assert.Equal(t, errcode.ERR_OK, r.ErrCode)
		}

		{
			r := c.DecrBy("test", "sniperHW", "age", 1, 10).Exec()
			assert.Equal(t, errcode.ERR_INVAILD_TABLE, r.ErrCode)
		}

		{
			r := c.DecrBy("users1", "sniperHW", "age1", 1, 10).Exec()
			assert.Equal(t, errcode.ERR_INVAILD_FIELD, r.ErrCode)
		}

		{
			r := c.DecrBy("users1", "sniperHW", "age", 1).Exec()
			assert.Equal(t, errcode.ERR_OK, r.ErrCode)
			assert.Equal(t, int64(-1), r.Fields["age"].GetInt())
		}

		{
			r := c.DecrBy("users1", "sniperHW", "age", 1, 10).Exec()
			assert.Equal(t, errcode.ERR_VERSION_MISMATCH, r.ErrCode)
		}

		//compare
		{
			r := c.CompareAndSet("users1", "sniperHW", "age", -1, 100).Exec()
			assert.Equal(t, errcode.ERR_OK, r.ErrCode)
		}

		{
			r := c.CompareAndSet("users1", "sniperHW", "age", -1, 100, 1000).Exec()
			assert.Equal(t, errcode.ERR_VERSION_MISMATCH, r.ErrCode)
		}

		{
			r := c.CompareAndSet("test", "sniperHW", "age", 1, 10).Exec()
			assert.Equal(t, errcode.ERR_INVAILD_TABLE, r.ErrCode)
		}

		{
			r := c.CompareAndSet("users1", "sniperHW", "age1", 1, 10).Exec()
			assert.Equal(t, errcode.ERR_INVAILD_FIELD, r.ErrCode)
		}

		{
			r := c.Del("users1", "sniperHW").Exec()
			assert.Equal(t, errcode.ERR_OK, r.ErrCode)
		}

		{
			r := c.CompareAndSet("users1", "sniperHW", "age", -1, 100).Exec()
			assert.Equal(t, errcode.ERR_RECORD_NOTEXIST, r.ErrCode)
		}

		{
			r := c.CompareAndSetNx("users1", "sniperHW", "age", -1, 100).Exec()
			assert.Equal(t, errcode.ERR_OK, r.ErrCode)
		}

		{
			r := c.CompareAndSetNx("users1", "sniperHW", "age", -1, 100, 1000).Exec()
			assert.Equal(t, errcode.ERR_VERSION_MISMATCH, r.ErrCode)
		}

		{
			r := c.CompareAndSetNx("test", "sniperHW", "age", 1, 10).Exec()
			assert.Equal(t, errcode.ERR_INVAILD_TABLE, r.ErrCode)
		}

		{
			r := c.CompareAndSetNx("users1", "sniperHW", "age1", 1, 10).Exec()
			assert.Equal(t, errcode.ERR_INVAILD_FIELD, r.ErrCode)
		}

		{
			r := c.CompareAndSet("users1", "sniperHW", "age", 101, 1).Exec()
			assert.Equal(t, errcode.ERR_CAS_NOT_EQUAL, r.ErrCode)
		}

		{
			r := c.CompareAndSetNx("users1", "sniperHW", "age", 101, 1).Exec()
			assert.Equal(t, errcode.ERR_CAS_NOT_EQUAL, r.ErrCode)
		}

		{
			r := c.CompareAndSetNx("users1", "sniperHW", "age", 100, 1).Exec()
			assert.Equal(t, errcode.ERR_OK, r.ErrCode)
		}

	}
}

func test2(t *testing.T, c *client.Client) {

	{
		//del
		//set
		fields := map[string]interface{}{}
		fields["age"] = 12
		fields["name"] = "sniperHW"

		r1 := c.Set("users1", "sniperHW", fields).Exec()
		assert.Equal(t, errcode.ERR_OK, r1.ErrCode)

		r2 := c.Del("users1", "sniperHW").Exec()
		assert.Equal(t, errcode.ERR_OK, r2.ErrCode)

		//kick
		for {
			r := c.Kick("users1", "sniperHW").Exec()
			if r.ErrCode == errcode.ERR_OK {
				break
			}
			time.Sleep(time.Millisecond * 100)
		}

		r2 = c.Del("users1", "sniperHW").Exec()
		assert.Equal(t, errcode.ERR_RECORD_NOTEXIST, r2.ErrCode)

		r1 = c.Set("users1", "sniperHW", fields).Exec()
		assert.Equal(t, errcode.ERR_OK, r1.ErrCode)

		//kick
		for {
			r := c.Kick("users1", "sniperHW").Exec()
			if r.ErrCode == errcode.ERR_OK {
				break
			}
			time.Sleep(time.Millisecond * 100)
		}

		r2 = c.Del("users1", "sniperHW").Exec()
		assert.Equal(t, errcode.ERR_OK, r2.ErrCode)

		r2 = c.Del("users1", "sniperHW").Exec()
		assert.Equal(t, errcode.ERR_RECORD_NOTEXIST, r2.ErrCode)

	}

	{
		//set

		fields := map[string]interface{}{}
		fields["age"] = 12
		fields["name"] = "sniperHW"

		r1 := c.Set("users1", "sniperHW", fields).Exec()
		assert.Equal(t, errcode.ERR_OK, r1.ErrCode)

		r2 := c.GetAll("users1", "sniperHW").Exec()
		assert.Equal(t, errcode.ERR_OK, r2.ErrCode)
		assert.Equal(t, "sniperHW", r2.Fields["name"].GetString())
		assert.Equal(t, int64(12), r2.Fields["age"].GetInt())

		r2 = c.GetAllWithVersion("users1", "sniperHW", r2.Version).Exec()
		assert.Equal(t, errcode.ERR_RECORD_UNCHANGE, r2.ErrCode)

		//kick
		for {
			r := c.Kick("users1", "sniperHW").Exec()
			if r.ErrCode == errcode.ERR_OK {
				break
			}
			time.Sleep(time.Millisecond * 100)
		}

		r2 = c.GetAllWithVersion("users1", "sniperHW", r2.Version).Exec()
		assert.Equal(t, errcode.ERR_RECORD_UNCHANGE, r2.ErrCode)

		r2 = c.Get("users1", "sniperHW", "aa").Exec()
		assert.Equal(t, errcode.ERR_INVAILD_FIELD, r2.ErrCode)

		//kick
		for {
			r := c.Kick("users1", "sniperHW").Exec()
			if r.ErrCode == errcode.ERR_OK {
				break
			}
			time.Sleep(time.Millisecond * 100)
		}

		r1 = c.Set("users1", "sniperHW", fields).Exec()
		assert.Equal(t, errcode.ERR_OK, r1.ErrCode)

		//kick
		for {
			r := c.Kick("users1", "sniperHW").Exec()
			if r.ErrCode == errcode.ERR_OK {
				break
			}
			time.Sleep(time.Millisecond * 100)
		}

		r1 = c.Set("users1", "sniperHW", fields, 100).Exec()
		assert.Equal(t, errcode.ERR_VERSION_MISMATCH, r1.ErrCode)

		fields["aa"] = "sniperHW"
		r1 = c.Set("users1", "sniperHW", fields).Exec()
		assert.Equal(t, errcode.ERR_INVAILD_FIELD, r1.ErrCode)

	}

	{

		//CompareAndSetNx
		r1 := c.CompareAndSetNx("users1", "sniperHW", "age", 1, 10).Exec()
		assert.Equal(t, errcode.ERR_CAS_NOT_EQUAL, r1.ErrCode)

		r2 := c.CompareAndSetNx("users1", "sniperHW", "age", 12, 10).Exec()
		assert.Equal(t, errcode.ERR_OK, r2.ErrCode)

		r3 := c.GetAll("users1", "sniperHW").Exec()
		assert.Equal(t, errcode.ERR_OK, r3.ErrCode)
		assert.Equal(t, int64(10), r3.Fields["age"].GetInt())

		r4 := c.Del("users1", "sniperHW").Exec()
		assert.Equal(t, errcode.ERR_OK, r4.ErrCode)

		r5 := c.CompareAndSetNx("users1", "sniperHW", "age", 1, 100).Exec()
		assert.Equal(t, errcode.ERR_OK, r5.ErrCode)

		r6 := c.GetAll("users1", "sniperHW").Exec()
		assert.Equal(t, errcode.ERR_OK, r6.ErrCode)
		assert.Equal(t, int64(100), r6.Fields["age"].GetInt())

		c.Del("users1", "sniperHW").Exec()
		//kick
		for {
			r := c.Kick("users1", "sniperHW").Exec()
			if r.ErrCode == errcode.ERR_OK {
				break
			}
			time.Sleep(time.Millisecond * 100)
		}

		r5 = c.CompareAndSetNx("users1", "sniperHW", "age", 1, 100).Exec()
		assert.Equal(t, errcode.ERR_OK, r5.ErrCode)

		//kick
		for {
			r := c.Kick("users1", "sniperHW").Exec()
			if r.ErrCode == errcode.ERR_OK {
				break
			}
			time.Sleep(time.Millisecond * 100)
		}

		r5 = c.CompareAndSetNx("users1", "sniperHW", "age", 1, 100, 100).Exec()
		assert.Equal(t, errcode.ERR_VERSION_MISMATCH, r5.ErrCode)

		fields := map[string]interface{}{}
		fields["age"] = 12
		fields["name"] = "sniperHW"

		c.Set("users1", "sniperHW", fields).Exec()

		//kick
		for {
			r := c.Kick("users1", "sniperHW").Exec()
			if r.ErrCode == errcode.ERR_OK {
				break
			}
			time.Sleep(time.Millisecond * 100)
		}

		r5 = c.CompareAndSetNx("users1", "sniperHW", "age", 1, 100).Exec()
		assert.Equal(t, errcode.ERR_CAS_NOT_EQUAL, r5.ErrCode)

		for {
			r := c.Kick("users1", "sniperHW").Exec()
			if r.ErrCode == errcode.ERR_OK {
				break
			}
			time.Sleep(time.Millisecond * 100)
		}
		r5 = c.CompareAndSetNx("users1", "sniperHW", "age", 12, 10).Exec()
		assert.Equal(t, errcode.ERR_OK, r5.ErrCode)

		r5 = c.CompareAndSetNx("users1", "sniperHW", "bb", 1, 10).Exec()
		assert.Equal(t, errcode.ERR_INVAILD_FIELD, r5.ErrCode)

	}

	{
		//setNx
		fields := map[string]interface{}{}
		fields["age"] = 12
		fields["name"] = "sniperHW"

		r1 := c.SetNx("users1", "sniperHW", fields).Exec()
		assert.Equal(t, errcode.ERR_RECORD_EXIST, r1.ErrCode)
		assert.Equal(t, "sniperHW", r1.Fields["name"].GetString())

		//kick
		for {
			r := c.Kick("users1", "sniperHW").Exec()
			if r.ErrCode == errcode.ERR_OK {
				break
			}
			time.Sleep(time.Millisecond * 100)
		}

		r1 = c.SetNx("users1", "sniperHW", fields).Exec()
		assert.Equal(t, errcode.ERR_RECORD_EXIST, r1.ErrCode)
		assert.Equal(t, "sniperHW", r1.Fields["name"].GetString())

		r2 := c.Del("users1", "sniperHW", 100).Exec()
		assert.Equal(t, errcode.ERR_VERSION_MISMATCH, r2.ErrCode)

		r2 = c.Del("users1", "sniperHW").Exec()
		assert.Equal(t, errcode.ERR_OK, r2.ErrCode)

		r3 := c.SetNx("users1", "sniperHW", fields).Exec()
		assert.Equal(t, errcode.ERR_OK, r3.ErrCode)

		c.Del("users1", "sniperHW").Exec()
		//kick
		for {
			r := c.Kick("users1", "sniperHW").Exec()
			if r.ErrCode == errcode.ERR_OK {
				break
			}
			time.Sleep(time.Millisecond * 100)
		}

		r3 = c.SetNx("users1", "sniperHW", fields).Exec()
		assert.Equal(t, errcode.ERR_OK, r3.ErrCode)

		//kick
		for {
			r := c.Kick("users1", "sniperHW").Exec()
			if r.ErrCode == errcode.ERR_OK {
				break
			}
			time.Sleep(time.Millisecond * 100)
		}

		r3 = c.SetNx("users1", "sniperHW", fields).Exec()
		assert.Equal(t, errcode.ERR_RECORD_EXIST, r3.ErrCode)

		fields["aa"] = "sniperHW"
		r3 = c.SetNx("users1", "sniperHW", fields).Exec()
		assert.Equal(t, errcode.ERR_INVAILD_FIELD, r3.ErrCode)

	}

	{
		//incr/decr
		r1 := c.GetAll("users1", "sniperHW").Exec()
		assert.Equal(t, errcode.ERR_OK, r1.ErrCode)
		age := r1.Fields["age"].GetInt()

		r2 := c.IncrBy("users1", "sniperHW", "age", 1).Exec()
		assert.Equal(t, errcode.ERR_OK, r2.ErrCode)
		assert.Equal(t, age+1, r2.Fields["age"].GetInt())
		age = r2.Fields["age"].GetInt()

		r3 := c.DecrBy("users1", "sniperHW", "age", 1).Exec()
		assert.Equal(t, errcode.ERR_OK, r3.ErrCode)
		assert.Equal(t, age-1, r3.Fields["age"].GetInt())

		//del and kick
		c.Del("users1", "sniperHW").Exec()
		for {
			r := c.Kick("users1", "sniperHW").Exec()
			if r.ErrCode == errcode.ERR_OK {
				break
			}
			time.Sleep(time.Millisecond * 100)
		}

		r2 = c.IncrBy("users1", "sniperHW", "age", 1).Exec()
		assert.Equal(t, errcode.ERR_OK, r2.ErrCode)

		//del and kick
		c.Del("users1", "sniperHW").Exec()
		for {
			r := c.Kick("users1", "sniperHW").Exec()
			if r.ErrCode == errcode.ERR_OK {
				break
			}
			time.Sleep(time.Millisecond * 100)
		}

		r3 = c.DecrBy("users1", "sniperHW", "age", 1).Exec()
		assert.Equal(t, errcode.ERR_OK, r3.ErrCode)

		for {
			r := c.Kick("users1", "sniperHW").Exec()
			if r.ErrCode == errcode.ERR_OK {
				break
			}
			time.Sleep(time.Millisecond * 100)
		}

		r3 = c.DecrBy("users1", "sniperHW", "age", 1, 100).Exec()
		assert.Equal(t, errcode.ERR_VERSION_MISMATCH, r3.ErrCode)

		for {
			r := c.Kick("users1", "sniperHW").Exec()
			if r.ErrCode == errcode.ERR_OK {
				break
			}
			time.Sleep(time.Millisecond * 100)
		}

		r3 = c.DecrBy("users1", "sniperHW", "age", 1).Exec()
		assert.Equal(t, errcode.ERR_OK, r3.ErrCode)

		r3 = c.DecrBy("users1", "sniperHW", "a", 1).Exec()
		assert.Equal(t, errcode.ERR_INVAILD_FIELD, r3.ErrCode)

		r3 = c.IncrBy("users1", "sniperHW", "a", 1).Exec()
		assert.Equal(t, errcode.ERR_INVAILD_FIELD, r3.ErrCode)

		c.Del("users1", "sniperHW").Exec()

		r3 = c.DecrBy("users1", "sniperHW", "age", 1).Exec()
		assert.Equal(t, errcode.ERR_OK, r3.ErrCode)

		c.Del("users1", "sniperHW").Exec()

		r3 = c.IncrBy("users1", "sniperHW", "age", 1).Exec()
		assert.Equal(t, errcode.ERR_OK, r3.ErrCode)

		for {
			r := c.Kick("users1", "sniperHW").Exec()
			if r.ErrCode == errcode.ERR_OK {
				break
			}
			time.Sleep(time.Millisecond * 100)
		}

		c.IncrBy("users1", "sniperHW", "age", 1).AsyncExec(func(_ *client.SliceResult) {})
		c.IncrBy("users1", "sniperHW", "age", 1).AsyncExec(func(_ *client.SliceResult) {})
		c.IncrBy("users1", "sniperHW", "age", 1).AsyncExec(func(_ *client.SliceResult) {})
		r3 = c.IncrBy("users1", "sniperHW", "age", 1).Exec()
		assert.Equal(t, errcode.ERR_OK, r3.ErrCode)
	}

	{
		//version
		fields := map[string]interface{}{}
		fields["age"] = 12
		fields["name"] = "sniperHW"

		c.SetNx("users1", "sniperHW", fields).Exec()
		r1 := c.GetAll("users1", "sniperHW").Exec()
		assert.Equal(t, errcode.ERR_OK, r1.ErrCode)
		version := r1.Version

		r2 := c.Set("users1", "sniperHW", fields).Exec()
		assert.Equal(t, errcode.ERR_OK, r2.ErrCode)
		assert.Equal(t, version+1, r2.Version)

		r3 := c.GetAllWithVersion("users1", "sniperHW", version+1).Exec()
		assert.Equal(t, errcode.ERR_RECORD_UNCHANGE, r3.ErrCode)

		r4 := c.Set("users1", "sniperHW", fields, version).Exec()
		assert.Equal(t, errcode.ERR_VERSION_MISMATCH, r4.ErrCode)

	}

	{
		//reload
		for {
			r := c.ReloadTableConf().Exec()
			if r.ErrCode == errcode.ERR_OK {
				break
			}
			time.Sleep(time.Millisecond * 100)
		}
	}

	{
		//kick
		for {
			r := c.Kick("users1", "sniperHW").Exec()
			if r.ErrCode == errcode.ERR_OK {
				break
			}
			time.Sleep(time.Millisecond * 100)
		}
		//again
		for {
			r := c.Kick("users1", "sniperHW").Exec()
			if r.ErrCode == errcode.ERR_OK {
				break
			}
			time.Sleep(time.Millisecond * 100)
		}
	}

	{

		fields := map[string]interface{}{}
		fields["age"] = 12
		fields["name"] = "sniperHW"

		c.Set("users1", "sniperHW", fields).AsyncExec(func(_ *client.StatusResult) {})
		c.Set("users1", "sniperHW", fields).AsyncExec(func(_ *client.StatusResult) {})
		c.Set("users1", "sniperHW", fields).AsyncExec(func(_ *client.StatusResult) {})
		c.GetAll("users1", "sniperHW").AsyncExec(func(_ *client.SliceResult) {})
		c.GetAll("users1", "sniperHW").AsyncExec(func(_ *client.SliceResult) {})
		c.GetAll("users1", "sniperHW").Exec()

		c.Set("users1", "sniperHW", fields).AsyncExec(func(_ *client.StatusResult) {})
		c.Set("users1", "sniperHW", fields).AsyncExec(func(_ *client.StatusResult) {})
		c.Set("users1", "sniperHW", fields).AsyncExec(func(_ *client.StatusResult) {})
		c.IncrBy("users1", "sniperHW", "age", 1).AsyncExec(func(_ *client.SliceResult) {})
		c.IncrBy("users1", "sniperHW", "age", 1).AsyncExec(func(_ *client.SliceResult) {})
		c.GetAll("users1", "sniperHW").Exec()

		c.IncrBy("users1", "sniperHW", "age", 1).AsyncExec(func(_ *client.SliceResult) {})
		c.IncrBy("users1", "sniperHW", "age", 1).AsyncExec(func(_ *client.SliceResult) {})
		c.IncrBy("users1", "sniperHW", "age", 1, 100).AsyncExec(func(_ *client.SliceResult) {})
		c.GetAll("users1", "sniperHW").Exec()

		c.GetAll("users1", "sniperHW").AsyncExec(func(_ *client.SliceResult) {})
		c.GetAll("users1", "sniperHW").AsyncExec(func(_ *client.SliceResult) {})
		c.Set("users1", "sniperHW", fields).AsyncExec(func(_ *client.StatusResult) {})
		c.Set("users1", "sniperHW", fields, 100).AsyncExec(func(_ *client.StatusResult) {})
		c.GetAll("users1", "sniperHW").Exec()

		c.GetAll("users1", "sniperHW").AsyncExec(func(_ *client.SliceResult) {})
		c.GetAll("users1", "sniperHW").AsyncExec(func(_ *client.SliceResult) {})
		c.SetNx("users1", "sniperHW", fields).AsyncExec(func(_ *client.SliceResult) {})
		c.GetAll("users1", "sniperHW").Exec()

		c.GetAll("users1", "sniperHW").AsyncExec(func(_ *client.SliceResult) {})
		c.GetAll("users1", "sniperHW").AsyncExec(func(_ *client.SliceResult) {})
		c.CompareAndSet("users1", "sniperHW", "age", 12, 10).AsyncExec(func(_ *client.SliceResult) {})
		c.GetAll("users1", "sniperHW").Exec()

		c.GetAll("users1", "sniperHW").AsyncExec(func(_ *client.SliceResult) {})
		c.GetAll("users1", "sniperHW").AsyncExec(func(_ *client.SliceResult) {})
		c.CompareAndSetNx("users1", "sniperHW", "age", 12, 10).AsyncExec(func(_ *client.SliceResult) {})
		c.GetAll("users1", "sniperHW").Exec()

		c.GetAll("users1", "sniperHW").AsyncExec(func(_ *client.SliceResult) {})
		c.GetAll("users1", "sniperHW").AsyncExec(func(_ *client.SliceResult) {})
		c.Del("users1", "sniperHW").AsyncExec(func(_ *client.StatusResult) {})
		c.GetAll("users1", "sniperHW").Exec()

	}

	{
		//CompareAndSet
		fields := map[string]interface{}{}
		fields["age"] = 12
		fields["name"] = "sniperHW"

		r1 := c.Set("users1", "sniperHW", fields).Exec()
		assert.Equal(t, errcode.ERR_OK, r1.ErrCode)

		r2 := c.CompareAndSet("users1", "sniperHW", "age", 12, 10).Exec()
		assert.Equal(t, errcode.ERR_OK, r2.ErrCode)

		r3 := c.CompareAndSet("users1", "sniperHW", "age", 12, 10).Exec()
		assert.Equal(t, errcode.ERR_CAS_NOT_EQUAL, r3.ErrCode)
		assert.Equal(t, r3.Fields["age"].GetInt(), int64(10))

		//kick
		for {
			r := c.Kick("users1", "sniperHW").Exec()
			if r.ErrCode == errcode.ERR_OK {
				break
			}
			time.Sleep(time.Millisecond * 100)
		}

		r2 = c.CompareAndSet("users1", "sniperHW", "age", 10, 20).Exec()
		assert.Equal(t, errcode.ERR_OK, r2.ErrCode)

		r2 = c.CompareAndSet("users1", "sniperHW", "age", 20, 10, 1).Exec()
		assert.Equal(t, errcode.ERR_VERSION_MISMATCH, r2.ErrCode)

		//kick
		for {
			r := c.Kick("users1", "sniperHW").Exec()
			if r.ErrCode == errcode.ERR_OK {
				break
			}
			time.Sleep(time.Millisecond * 100)
		}

		r2 = c.CompareAndSet("users1", "sniperHW", "age", 12, 10, 1).Exec()
		assert.Equal(t, errcode.ERR_VERSION_MISMATCH, r2.ErrCode)

		//kick
		for {
			r := c.Kick("users1", "sniperHW").Exec()
			if r.ErrCode == errcode.ERR_OK {
				break
			}
			time.Sleep(time.Millisecond * 100)
		}

		r2 = c.CompareAndSet("users1", "sniperHW", "age", 12, 10).Exec()
		assert.Equal(t, errcode.ERR_CAS_NOT_EQUAL, r2.ErrCode)

		r2 = c.CompareAndSet("users1", "sniperHW", "bb", 12, 10).Exec()
		assert.Equal(t, errcode.ERR_INVAILD_FIELD, r2.ErrCode)

		//del and kick
		c.Del("users1", "sniperHW").Exec()
		for {
			r := c.Kick("users1", "sniperHW").Exec()
			if r.ErrCode == errcode.ERR_OK {
				break
			}
			time.Sleep(time.Millisecond * 100)
		}

		r2 = c.CompareAndSet("users1", "sniperHW", "age", 12, 10).Exec()
		assert.Equal(t, errcode.ERR_RECORD_NOTEXIST, r2.ErrCode)

	}

	{
		fields := map[string]interface{}{}
		fields["age"] = 12
		fields["name"] = "sniperHW2"

		c.SetNx("users1", "sniperHW2", fields).Exec()
	}

}

func TestMysql(t *testing.T) {

	dbConf := &dbconf{}
	if _, err := toml.DecodeFile("test_dbconf.toml", dbConf); nil != err {
		panic(err)
	}

	//先删除所有kv文件
	os.RemoveAll("./kv-1-1")
	os.RemoveAll("./kv-1-1-snap")

	//conf.LoadConfigStr(fmt.Sprintf(configStr, 10012, "mysql", "localhost", 3306, "root", "123456", "huangwei", "localhost", 3306, "root", "123456", "huangwei"))

	conf.LoadConfigStr(fmt.Sprintf(configStr, 10012, "mysql", "localhost", 3306, dbConf.MysqlUser, dbConf.MysqlPwd, dbConf.MysqlDb, "localhost", 3306, dbConf.MysqlUser, dbConf.MysqlPwd, dbConf.MysqlDb))

	InitLogger()
	UpdateLogConfig()

	cluster := "1@http://127.0.0.1:12376"
	id := 1

	node := NewKvNode()

	if err := node.Start(&id, &cluster); nil != err {
		panic(err)
	}

	//等到所有store都成为leader之后再发送指令
	waitCondition(func() bool {
		node.storeMgr.RLock()
		defer node.storeMgr.RUnlock()
		for _, v := range node.storeMgr.stores {
			if !v.rn.isLeader() {
				return false
			}
		}
		return true
	})

	c := client.OpenClient("localhost:10012", false)
	test1(t, c)
	test2(t, c)

	node.Stop()

	time.Sleep(time.Second)
}

func TestKvnode1(t *testing.T) {

	dbConf := &dbconf{}
	if _, err := toml.DecodeFile("test_dbconf.toml", dbConf); nil != err {
		panic(err)
	}

	//先删除所有kv文件
	os.RemoveAll("./kv-1-1")
	os.RemoveAll("./kv-1-1-snap")

	conf.LoadConfigStr(fmt.Sprintf(configStr, 10018, "pgsql", "localhost", 5432, dbConf.PgUser, dbConf.PgPwd, dbConf.PgDB, "localhost", 5432, dbConf.PgUser, dbConf.PgPwd, dbConf.PgDB))

	InitLogger()
	UpdateLogConfig()

	cluster := "1@http://127.0.0.1:12377"
	id := 1

	node := NewKvNode()

	if err := node.Start(&id, &cluster); nil != err {
		panic(err)
	}

	//等到所有store都成为leader之后再发送指令
	waitCondition(func() bool {
		node.storeMgr.RLock()
		defer node.storeMgr.RUnlock()
		for _, v := range node.storeMgr.stores {
			if !v.rn.isLeader() {
				return false
			}
		}
		return true
	})

	c := client.OpenClient("localhost:10018", false)
	test1(t, c)
	test2(t, c)

	//测试配置变更
	{
		c.Del("users1", "sniperHW").Exec()

		fields := map[string]interface{}{}
		fields["age"] = 12
		fields["name"] = "sniperHW"

		r1 := c.Set("users1", "sniperHW", fields).Exec()
		assert.Equal(t, errcode.ERR_OK, r1.ErrCode)

		node.storeMgr.dbmeta.Reload([]string{"users1@name:string:,age:int:,phone:string:,test:string:justtest"})

		r2 := c.GetAll("users1", "sniperHW").Exec()
		assert.Equal(t, errcode.ERR_OK, r2.ErrCode)
		assert.Equal(t, "justtest", r2.Fields["test"].GetString())

	}
	node.Stop()

	cluster = "1@http://127.0.0.1:12378"

	node = NewKvNode()

	if err := node.Start(&id, &cluster); nil != err {
		panic(err)
	}

	//等到所有store都成为leader之后再发送指令
	waitCondition(func() bool {
		node.storeMgr.RLock()
		defer node.storeMgr.RUnlock()
		for _, v := range node.storeMgr.stores {
			if !v.rn.isLeader() {
				return false
			}
		}
		return true
	})

	node.Stop()

	time.Sleep(time.Second)

}

func TestKvnode2(t *testing.T) {

	dbConf := &dbconf{}
	if _, err := toml.DecodeFile("test_dbconf.toml", dbConf); nil != err {
		panic(err)
	}

	//先删除所有kv文件
	os.RemoveAll("./kv-1-1")
	os.RemoveAll("./kv-1-1-snap")

	conf.LoadConfigStr(fmt.Sprintf(configStr, 10018, "pgsql", "localhost", 5432, dbConf.PgUser, dbConf.PgPwd, dbConf.PgDB, "localhost", 5432, dbConf.PgUser, dbConf.PgPwd, dbConf.PgDB))

	InitLogger()

	cluster := "1@http://127.0.0.1:12379"
	id := 1

	node := NewKvNode()

	if err := node.Start(&id, &cluster); nil != err {
		panic(err)
	}

	//等到所有store都成为leader之后再发送指令
	waitCondition(func() bool {
		node.storeMgr.RLock()
		defer node.storeMgr.RUnlock()
		for _, v := range node.storeMgr.stores {
			if !v.rn.isLeader() {
				return false
			}
		}
		return true
	})

	c := client.OpenClient("localhost:10018", false)
	test1(t, c)
	test2(t, c)

	for i := 0; i < 10; i++ {
		n := fmt.Sprintf("test:%d", i)
		c.GetAll("users1", n).Exec()
	}

	node.storeMgr.RLock()
	for _, v := range node.storeMgr.stores {
		v.doLRU()
	}
	node.storeMgr.RUnlock()

	for i := 0; i < 10; i++ {
		n := fmt.Sprintf("test:%d", i)
		c.Kick("users1", n).Exec()
	}

	//写入一个大字段触发快照压缩
	fields := map[string]interface{}{}
	fields["age"] = 12
	fields["name"] = "sniperHW"
	fields["phone"] = strings.Repeat("a", 4096)

	r1 := c.Set("users1", "sniperHW", fields).Exec()
	assert.Equal(t, errcode.ERR_OK, r1.ErrCode)

	node.storeMgr.RLock()
	for _, v := range node.storeMgr.stores {
		v.rn.triggerSnapshot()
	}
	node.storeMgr.RUnlock()

	node.Stop()

	time.Sleep(time.Second)

}

func TestCluster(t *testing.T) {

	dbConf := &dbconf{}
	if _, err := toml.DecodeFile("test_dbconf.toml", dbConf); nil != err {
		panic(err)
	}

	os.RemoveAll("./kv-1-1")
	os.RemoveAll("./kv-1-1-snap")
	os.RemoveAll("./kv-2-1")
	os.RemoveAll("./kv-2-1-snap")
	os.RemoveAll("./kv-3-1")
	os.RemoveAll("./kv-3-1-snap")
	os.RemoveAll("./kv-4-1")
	os.RemoveAll("./kv-4-1-snap")

	conf.LoadConfigStr(fmt.Sprintf(configStr, 20018, "pgsql", "localhost", 5432, dbConf.PgUser, dbConf.PgPwd, dbConf.PgDB, "localhost", 5432, dbConf.PgUser, dbConf.PgPwd, dbConf.PgDB))

	InitLogger()

	cluster := "1@http://127.0.0.1:22378,2@http://127.0.0.1:22379,3@http://127.0.0.1:22380"
	id1 := 1

	node1 := NewKvNode()

	if err := node1.Start(&id1, &cluster); nil != err {
		panic(err)
	}

	conf.LoadConfigStr(fmt.Sprintf(configStr, 20019, "pgsql", "localhost", 5432, dbConf.PgUser, dbConf.PgPwd, dbConf.PgDB, "localhost", 5432, dbConf.PgUser, dbConf.PgPwd, dbConf.PgDB))

	id2 := 2

	node2 := NewKvNode()

	if err := node2.Start(&id2, &cluster); nil != err {
		panic(err)
	}

	conf.LoadConfigStr(fmt.Sprintf(configStr, 20020, "pgsql", "localhost", 5432, dbConf.PgUser, dbConf.PgPwd, dbConf.PgDB, "localhost", 5432, dbConf.PgUser, dbConf.PgPwd, dbConf.PgDB))

	id3 := 3

	node3 := NewKvNode()

	if err := node3.Start(&id3, &cluster); nil != err {
		panic(err)
	}

	isLeader1 := func() bool {
		if atomic.LoadInt32(&node1.stoped) == 1 {
			return false
		}

		node1.storeMgr.RLock()
		defer node1.storeMgr.RUnlock()
		for _, v := range node1.storeMgr.stores {
			if v.rn.isLeader() {
				return true
			}
		}
		return false
	}

	isLeader2 := func() bool {

		if atomic.LoadInt32(&node2.stoped) == 1 {
			return false
		}

		node2.storeMgr.RLock()
		defer node2.storeMgr.RUnlock()
		for _, v := range node2.storeMgr.stores {
			if v.rn.isLeader() {
				return true
			}
		}
		return false
	}

	isLeader3 := func() bool {

		if atomic.LoadInt32(&node3.stoped) == 1 {
			return false
		}

		node3.storeMgr.RLock()
		defer node3.storeMgr.RUnlock()
		for _, v := range node3.storeMgr.stores {
			if v.rn.isLeader() {
				return true
			}
		}
		return false
	}

	getLeader := func() *KVNode {
		for {
			if isLeader1() {
				return node1
			}

			if isLeader2() {
				return node2
			}

			if isLeader3() {
				return node3
			}
			time.Sleep(time.Millisecond * 100)
		}
	}

	c1 := client.OpenClient("localhost:20018", false)
	c2 := client.OpenClient("localhost:20019", false)
	c3 := client.OpenClient("localhost:20020", false)

	getClient := func() *client.Client {
		for {
			if isLeader1() {
				return c1
			}

			if isLeader2() {
				return c2
			}

			if isLeader3() {
				return c3
			}
			time.Sleep(time.Millisecond * 100)
		}
	}

	getNoLeaderClient := func() *client.Client {
		if !isLeader1() {
			return c1
		}

		if !isLeader2() {
			return c2
		}

		if !isLeader3() {
			return c3
		}

		return nil
	}

	for i := 0; i < 20; i++ {
		c := getClient()
		fields := map[string]interface{}{}
		fields["age"] = 12
		fields["name"] = "sniperHW"
		r1 := c.Set("users1", "sniperHW", fields).Exec()
		assert.Equal(t, errcode.ERR_OK, r1.ErrCode)
	}

	{
		c := getClient()
		for {
			r := c.Kick("users1", "sniperHW").Exec()
			if r.ErrCode == errcode.ERR_OK {
				break
			}
			time.Sleep(time.Millisecond * 100)
		}
	}

	{
		c := getClient()
		r1 := c.GetAll("users1", "sniperHW").Exec()
		assert.Equal(t, errcode.ERR_OK, r1.ErrCode)
	}

	{
		c := getNoLeaderClient()
		fields := map[string]interface{}{}
		fields["age"] = 12
		fields["name"] = "sniperHW"
		r := c.Set("users1", "sniperHW", fields).Exec()
		assert.Equal(t, errcode.ERR_NOT_LEADER, r.ErrCode)
	}

	{

		c := getClient()
		r1 := c.GetAll("users1", "sniperHW").Exec()
		assert.Equal(t, errcode.ERR_OK, r1.ErrCode)
		version := r1.Version

		//模拟leader停机
		leader := getLeader()
		leader.Stop()

		//取保从新leader取到的数据与之前的一致
		c = getClient()
		r1 = c.GetAll("users1", "sniperHW").Exec()
		if r1.ErrCode == errcode.ERR_OK {
			assert.Equal(t, version, r1.Version)
		} else {
			assert.Equal(t, errcode.ERR_TIMEOUT, r1.ErrCode)
		}

		//停机节点恢复

		if leader == node1 {

			conf.LoadConfigStr(fmt.Sprintf(configStr, 20018, "pgsql", "localhost", 5432, dbConf.PgUser, dbConf.PgPwd, dbConf.PgDB, "localhost", 5432, dbConf.PgUser, dbConf.PgPwd, dbConf.PgDB))

			node1 = NewKvNode()

			if err := node1.Start(&id1, &cluster); nil != err {
				panic(err)
			}
		} else if leader == node2 {
			conf.LoadConfigStr(fmt.Sprintf(configStr, 20019, "pgsql", "localhost", 5432, dbConf.PgUser, dbConf.PgPwd, dbConf.PgDB, "localhost", 5432, dbConf.PgUser, dbConf.PgPwd, dbConf.PgDB))

			node2 = NewKvNode()

			if err := node2.Start(&id2, &cluster); nil != err {
				panic(err)
			}
		} else {
			conf.LoadConfigStr(fmt.Sprintf(configStr, 20020, "pgsql", "localhost", 5432, dbConf.PgUser, dbConf.PgPwd, dbConf.PgDB, "localhost", 5432, dbConf.PgUser, dbConf.PgPwd, dbConf.PgDB))

			node3 = NewKvNode()

			if err := node3.Start(&id3, &cluster); nil != err {
				panic(err)
			}
		}

	}

	{
		cluster := "1@http://127.0.0.1:22378,2@http://127.0.0.1:22379,3@http://127.0.0.1:22380,4@http://127.0.0.1:22381"

		conf.LoadConfigStr(fmt.Sprintf(configStr, 20021, "pgsql", "localhost", 5432, dbConf.PgUser, dbConf.PgPwd, dbConf.PgDB, "localhost", 5432, dbConf.PgUser, dbConf.PgPwd, dbConf.PgDB))

		id4 := 4

		node4 := NewKvNode()

		if err := node4.Start(&id4, &cluster); nil != err {
			panic(err)
		}

		//请求leader添加一个新节点
		leader := getLeader()

		store := leader.storeMgr.getStoreByIndex(1)

		retC := make(chan int32)

		store.issueConfChange(&asynTaskConfChange{
			nodeid:     (4 << 16) + 1,
			changeType: raftpb.ConfChangeAddNode,
			url:        "http://127.0.0.1:22381",
			doneCB: func() {
				retC <- int32(0)
			},
			errorCB: func(errno int32) {
				retC <- errno
			},
		})

		ok := <-retC

		assert.Equal(t, int32(0), ok)

		c := getClient()
		fields := map[string]interface{}{}
		fields["age"] = 12
		fields["name"] = "sniperHW"
		r1 := c.Set("users1", "sniperHW", fields).Exec()
		assert.Equal(t, errcode.ERR_OK, r1.ErrCode)

		node4.Stop()

	}

	node1.Stop()
	node2.Stop()
	node3.Stop()

	time.Sleep(time.Second)

}
