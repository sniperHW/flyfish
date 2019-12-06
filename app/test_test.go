package flyfish

import (
	"fmt"
	"github.com/sniperHW/flyfish/client"
	"github.com/sniperHW/flyfish/errcode"
	"testing"
)

func TestGet(t *testing.T) {
	c := client.OpenClient("127.0.0.1:10012", false)

	get := c.Get("test", "test1")

	get.AsyncExec(func(ret *client.SliceResult) {
		if ret.ErrCode == errcode.ERR_OK {
			fmt.Println(ret.Key, ret.Fields)
		} else {
			fmt.Println(errcode.GetErrorStr(ret.ErrCode))
		}
	})

	sigStop := make(chan bool)
	_, _ = <-sigStop
}

func TestSet(t *testing.T) {
	c := client.OpenClient("127.0.0.1:10012", false)
	fields := map[string]interface{}{}
	fields["test"] = 5
	fields["test1"] = 3

	set := c.Set("test", "sdsf", fields)
	set.AsyncExec(func(ret *client.StatusResult) {

		if ret.ErrCode != errcode.ERR_OK {
			fmt.Println(errcode.GetErrorStr(ret.ErrCode))
		} else {
			fmt.Println("set ok")
		}
	})
	sigStop := make(chan bool)
	_, _ = <-sigStop
}
