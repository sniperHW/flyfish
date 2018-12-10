package main 

import(
	"fmt"
	kclient "flyfish/client"
	"flyfish/errcode"
	"github.com/sniperHW/kendynet/golog"
	"github.com/sniperHW/kendynet"
)

func IncrBy(c *kclient.Client) {
	incr := c.IncrBy("counter","test_counter1","c",1)
	incr.Exec(func(ret *kclient.Result) {

		if ret.ErrCode != errcode.ERR_OK {
			fmt.Println(errcode.GetErrorStr(ret.ErrCode))
		} else {
			fmt.Println("c:",ret.Fields["c"].GetInt())
		}
	})	
}


func main() {

	golog.DisableStdOut()
	outLogger := golog.NewOutputLogger("log", "flyfish get", 1024*1024*50)
	kendynet.InitLogger(outLogger,"flyfish get")

	c := kclient.OpenClient("localhost:10012")//eventQueue)

	IncrBy(c)

	//eventQueue.Run()

	sigStop := make(chan bool)
	_, _ = <-sigStop
}