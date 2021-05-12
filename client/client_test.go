package client

//go test -covermode=count -v -coverprofile=coverage.out -run=.
//go tool cover -html=coverage.out

import (
	"fmt"
	"github.com/sniperHW/flyfish/backend/db"
	"github.com/sniperHW/flyfish/errcode"
	"github.com/sniperHW/flyfish/logger"
	"github.com/sniperHW/flyfish/proto"
	"github.com/sniperHW/flyfish/server/mock/kvnode"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

var (
	Err_version_mismatch errcode.Error = errcode.New(errcode.Errcode_version_mismatch, "")
	Err_record_exist     errcode.Error = errcode.New(errcode.Errcode_record_exist, "")
	Err_record_notexist  errcode.Error = errcode.New(errcode.Errcode_record_notexist, "")
	Err_record_unchange  errcode.Error = errcode.New(errcode.Errcode_record_unchange, "")
	Err_cas_not_equal    errcode.Error = errcode.New(errcode.Errcode_cas_not_equal, "")
	Err_timeout          errcode.Error = errcode.New(errcode.Errcode_timeout, "timeout")
)

func init() {
	InitLogger(logger.NewZapLogger("client.log", "./log", "debug", 100, 14, true))
}

func test(t *testing.T, c *Client) {

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
			ok := make(chan struct{})

			c.Set("users1", "sniperHW", fields).AsyncExec(func(r *StatusResult) {
				assert.Nil(t, r.ErrCode)
				close(ok)
			})

			<-ok

		}

		{
			r := c.GetAll("users1", "sniperHW").Exec()
			assert.Nil(t, r.ErrCode)
			assert.Equal(t, "sniperHW", r.Fields["name"].GetString())
			assert.Equal(t, int64(12), r.Fields["age"].GetInt())
		}

		{
			ok := make(chan struct{})

			c.GetAll("users1", "sniperHW").AsyncExec(func(r *SliceResult) {
				assert.Nil(t, r.ErrCode)
				assert.Equal(t, "sniperHW", r.Fields["name"].GetString())
				assert.Equal(t, int64(12), r.Fields["age"].GetInt())
				close(ok)
			})

			<-ok

		}

		{
			r := c.Get("users1", "sniperHW", "name", "age").Exec()
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
			fmt.Println(r.ErrCode)
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
			assert.NotNil(t, r.ErrCode)
		}

		{

			fields := map[string]interface{}{}
			fields["ages"] = 12
			r := c.Set("users1", "sniperHW", fields).Exec()
			assert.NotNil(t, r.ErrCode)
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
			assert.NotNil(t, r.ErrCode)
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
			assert.NotNil(t, r.ErrCode)
		}

		{

			fields := map[string]interface{}{}
			fields["ages"] = 12
			r := c.SetNx("users1", "sniperHW", fields).Exec()
			assert.NotNil(t, r.ErrCode)
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
			assert.NotNil(t, r.ErrCode)
		}

		{
			r := c.IncrBy("users1", "sniperHW", "age1", 1, 10).Exec()
			assert.NotNil(t, r.ErrCode)
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
			assert.NotNil(t, r.ErrCode)
		}

		{
			r := c.DecrBy("users1", "sniperHW", "age1", 1, 10).Exec()
			assert.NotNil(t, r.ErrCode)
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
			assert.NotNil(t, r.ErrCode)
		}

		{
			r := c.CompareAndSet("users1", "sniperHW", "age1", 1, 10).Exec()
			assert.NotNil(t, r.ErrCode)
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
			assert.NotNil(t, r.ErrCode)
		}

		{
			r := c.CompareAndSetNx("users1", "sniperHW", "age1", 1, 10).Exec()
			assert.NotNil(t, r.ErrCode)
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

		{

			{
				fields := map[string]interface{}{}
				fields["age"] = 12
				fields["name"] = "sniperHW1"
				c.Set("users1", "sniperHW1", fields).Exec()
			}
			{
				fields := map[string]interface{}{}
				fields["age"] = 13
				fields["name"] = "sniperHW2"
				c.Set("users1", "sniperHW2", fields).Exec()
			}
			{
				fields := map[string]interface{}{}
				fields["age"] = 14
				fields["name"] = "sniperHW3"
				c.Set("users1", "sniperHW3", fields).Exec()
			}

			r4 := MGet(c.GetAll("users1", "sniperHW1"), c.GetAll("users1", "sniperHW2"), c.GetAll("users1", "sniperHW3")).Exec()

			for _, v := range r4 {
				if v.ErrCode == nil {
					fmt.Println(v.Table, v.Key, "age:", v.Fields["age"].GetInt())
				} else {
					fmt.Println(v.Table, v.Key, errcode.GetErrorDesc(v.ErrCode))
				}
			}
		}

	}
}

func testTimeout(t *testing.T, c *Client) {
	{
		fields := map[string]interface{}{}
		fields["age"] = 12
		fields["name"] = "sniperHW"

		r1 := c.Set("users1", "sniperHW", fields).Exec()
		assert.Equal(t, Err_timeout, r1.ErrCode)

		r0 := c.GetAll("users1", "sniperHW").Exec()
		assert.Equal(t, Err_timeout, r0.ErrCode)
	}
}

func testConnDisconnect(t *testing.T) {
	c := OpenClient("localhost:8110")

	ClientTimeout = 5000
	kvnode.SetProcessDelay(2 * time.Second)

	ok := make(chan struct{})

	c.GetWithVersion("users1", "sniperHW", 1, "age").AsyncExec(func(r *SliceResult) {
		assert.Equal(t, r.ErrCode.Desc, "lose connection")
		close(ok)
	})

	for c.conn.session == nil {
		time.Sleep(time.Second)
	}

	c.conn.session.Close(nil, 0)

	<-ok

	kvnode.SetProcessDelay(0)
}

func testMaxPendingSize(t *testing.T) {
	c := OpenClient("localhost:8110")

	backmaxPendingSize := maxPendingSize

	maxPendingSize = 1

	ok := make(chan struct{})

	c.GetWithVersion("users1", "sniperHW", 1, "age").AsyncExec(func(r *SliceResult) {
		assert.Nil(t, r.ErrCode)
		close(ok)
	})

	c.GetWithVersion("users1", "sniperHW", 1, "age").AsyncExec(func(r *SliceResult) {
		assert.Equal(t, r.ErrCode.Desc, "busy please retry later")
	})

	<-ok

	maxPendingSize = backmaxPendingSize

}

func testPassiveDisconnected(t *testing.T) {
	c := OpenClient("localhost:8110")

	ClientTimeout = 5000
	kvnode.SetDisconnectOnRecvMsg()

	ok := make(chan struct{})

	c.GetWithVersion("users1", "sniperHW", 1, "age").AsyncExec(func(r *SliceResult) {
		assert.Equal(t, r.ErrCode.Desc, "lose connection")
		close(ok)
	})

	<-ok

	kvnode.ClearDisconnectOnRecvMsg()

}

func testDialFailed(t *testing.T) {

	c := OpenClient("localhost:8119")
	ok := make(chan struct{})

	ClientTimeout = 3000

	c.GetWithVersion("users1", "sniperHW", 1, "age").AsyncExec(func(r *SliceResult) {
		assert.Equal(t, r.ErrCode, Err_timeout)
		close(ok)
	})

	<-ok

}

var metaStr string = `
{"TableDefs":[{"Name":"users1","Fields":[{"Name":"name","Type":"string"},{"Name":"age","Type":"int","DefautValue":"0"},{"Name":"phone","Type":"string"}]}]}
`

func TestClient(t *testing.T) {
	{
		f1 := (*Field)(proto.PackField("float", 1.2))
		assert.Equal(t, false, f1.IsNil())
		assert.Equal(t, f1.GetFloat(), 1.2)

		f2 := (*Field)(proto.PackField("blob", []byte("blob")))
		assert.Equal(t, []byte("blob"), f2.GetBlob())
		assert.Equal(t, []byte("blob"), f2.GetValue().([]byte))
	}
	def, _ := db.CreateDbDefFromJsonString([]byte(metaStr))
	n := kvnode.New()
	assert.Nil(t, n.Start("localhost:8110", def))
	c := OpenClient("localhost:8110")
	test(t, c)
	ClientTimeout = 1000
	kvnode.SetProcessDelay(2 * time.Second)
	testTimeout(t, c)
	kvnode.SetProcessDelay(0)
	testConnDisconnect(t)
	testMaxPendingSize(t)
	testPassiveDisconnected(t)
	testDialFailed(t)
	time.Sleep(time.Second)

	assert.Equal(t, 0, len(c.conn.waitResp))

}
