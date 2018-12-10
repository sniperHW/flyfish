package main 

import(
	"fmt"
	kclient "flyfish/client"
	"flyfish/errcode"
	"github.com/sniperHW/kendynet/golog"
	"github.com/sniperHW/kendynet"
	"strings"
)

func Set(c *kclient.Client) {
	fields := map[string]interface{}{}
	fields["age"] = 37
	fields["phone"] = strings.Repeat("a",1024)
	fields["name"] = "sniperHW"
	key := fmt.Sprintf("%s:%d","huangwei",1)

	set := c.Set("users1",key,fields)
	set.Exec(func(ret *kclient.Result) {

		if ret.ErrCode != errcode.ERR_OK {
			fmt.Println(errcode.GetErrorStr(ret.ErrCode))
		} else {
			fmt.Println("set ok")
		}
	})
}


func main() {

	golog.DisableStdOut()
	outLogger := golog.NewOutputLogger("log", "flyfish get", 1024*1024*50)
	kendynet.InitLogger(outLogger,"flyfish get")

	c := kclient.OpenClient("localhost:10012")//eventQueue)

	Set(c)

	//eventQueue.Run()

	sigStop := make(chan bool)
	_, _ = <-sigStop
}