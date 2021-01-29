package client

//go test -covermode=count -v -coverprofile=coverage.out -run=.
//go tool cover -html=coverage.out

import (
	"fmt"
	"github.com/sniperHW/flyfish/errcode"
	"github.com/sniperHW/flyfish/mock_kvnode"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func test(t *testing.T, c *Client) {

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
			ok := make(chan struct{})

			c.Set("users1", "sniperHW", fields).AsyncExec(func(r *StatusResult) {
				assert.Equal(t, errcode.ERR_OK, r.ErrCode)
				close(ok)
			})

			<-ok

		}

		{
			r := c.GetAll("users1", "sniperHW").Exec()
			assert.Equal(t, errcode.ERR_OK, r.ErrCode)
			assert.Equal(t, "sniperHW", r.Fields["name"].GetString())
			assert.Equal(t, int64(12), r.Fields["age"].GetInt())
		}

		{
			ok := make(chan struct{})

			c.GetAll("users1", "sniperHW").AsyncExec(func(r *SliceResult) {
				assert.Equal(t, errcode.ERR_OK, r.ErrCode)
				assert.Equal(t, "sniperHW", r.Fields["name"].GetString())
				assert.Equal(t, int64(12), r.Fields["age"].GetInt())
				close(ok)
			})

			<-ok

		}

		{
			r := c.Get("users1", "sniperHW", "name", "age").Exec()
			assert.Equal(t, errcode.ERR_OK, r.ErrCode)
			assert.Equal(t, "sniperHW", r.Fields["name"].GetString())
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

		{
			r := c.Kick("users1", "sniperHW").Exec()
			assert.Equal(t, errcode.ERR_OK, r.ErrCode)
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
				if v.ErrCode == errcode.ERR_OK {
					fmt.Println(v.Table, v.Key, "age:", v.Fields["age"].GetInt())
				} else {
					fmt.Println(v.Table, v.Key, errcode.GetErrorStr(v.ErrCode))
				}
			}
		}

	}
}

func TestClient(t *testing.T) {
	n := mock_kvnode.New()
	assert.Nil(t, n.Start("localhost:8110", []string{"users1@name:string:,age:int:,phone:string:"}))
	c := OpenClient("localhost:8110", false)
	test(t, c)
	time.Sleep(time.Second)
}
