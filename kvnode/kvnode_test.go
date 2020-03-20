package kvnode

//go test -run=.

import (
	"encoding/binary"
	"github.com/sniperHW/flyfish/client"
	"github.com/sniperHW/flyfish/conf"
	"github.com/sniperHW/flyfish/errcode"
	"github.com/stretchr/testify/assert"
	"testing"
)

var configStr string = `
CacheGroupSize          = 1                  #cache分组数量，每一个cache组单独管理，以降低处理冲突

MaxCachePerGroupSize    = 100000               #每组最大key数量，超过数量将会触发key剔除

SqlLoadPipeLineSize     = 200                  #sql加载管道线大小

SqlLoadQueueSize        = 10000                #sql加载请求队列大小，此队列每CacheGroup一个

SqlLoaderCount          = 5
SqlUpdaterCount         = 5

StrInitCap              = 1048576 # 1mb

ServiceHost             = "127.0.0.1"

ServicePort             = 10018

ReplyBusyOnQueueFull    = false                #当处理队列满时是否用busy响应，如果填false,直接丢弃请求，让客户端超时 

Compress                = true

BatchByteSize           = 148576
BatchCount              = 200 
ProposalFlushInterval   = 100
ReadFlushInterval       = 10                   	 


[DBConfig]
SqlType         = "pgsql"


DbHost          = "localhost"
DbPort          = 5432
DbUser			= "sniper"
DbPassword      = "802802"
DbDataBase      = "test"

ConfDbHost      = "localhost"
ConfDbPort      = 5432
ConfDbUser      = "sniper"
ConfDbPassword  = "802802"
ConfDataBase    = "test"


#DbHost          = "10.128.2.166"
#DbPort          = 5432
#DbUser			= "dbuser"
#DbPassword      = "123456"
#DbDataBase      = "wei"

#ConfDbHost      = "10.128.2.166"
#ConfDbPort      = 5432
#ConfDbUser      = "dbuser"
#ConfDbPassword  = "123456"
#ConfDataBase    = "wei"


[Log]
MaxLogfileSize  = 104857600 # 100mb
LogDir          = "log1"
LogPrefix       = "flyfish"
LogLevel        = "info"
EnableLogStdout = true	
`

func init() {
	conf.LoadConfigStr(configStr)
	InitLogger()
	cluster := "http://127.0.0.1:12378"
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
}

func TestKvnode(t *testing.T) {

	c := client.OpenClient("localhost:10018", false)

	{
		//set
		buff := make([]byte, 4)

		binary.BigEndian.PutUint32(buff, 100)

		fields := map[string]interface{}{}
		fields["age"] = 12
		fields["name"] = "sniperHW"

		r1 := c.Set("users1", "sniperHW", fields).Exec()
		assert.Equal(t, errcode.ERR_OK, r1.ErrCode)

		r2 := c.GetAll("users1", "sniperHW").Exec()
		assert.Equal(t, errcode.ERR_OK, r2.ErrCode)
		assert.Equal(t, "sniperHW", r2.Fields["name"].GetString())
		assert.Equal(t, int64(12), r2.Fields["age"].GetInt())

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

	}

	{
		//setNx
		fields := map[string]interface{}{}
		fields["age"] = 12
		fields["name"] = "sniperHW"

		r1 := c.SetNx("users1", "sniperHW", fields).Exec()
		assert.Equal(t, errcode.ERR_RECORD_EXIST, r1.ErrCode)

		r2 := c.Del("users1", "sniperHW").Exec()
		assert.Equal(t, errcode.ERR_OK, r2.ErrCode)

		r3 := c.SetNx("users1", "sniperHW", fields).Exec()
		assert.Equal(t, errcode.ERR_OK, r3.ErrCode)

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

	}

}
