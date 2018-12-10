package main 

import(
	"fmt"
	kclient "flyfish/client"
	"flyfish/errcode"
	"github.com/sniperHW/kendynet/golog"
	"github.com/sniperHW/kendynet"
)

func CompareAndSet(c *kclient.Client) {
	set := c.CompareAndSet("counter","test_counter1","c",100,100)
	set.Exec(func(ret *kclient.Result) {

		if ret.ErrCode != errcode.ERR_OK {
			fmt.Println(errcode.GetErrorStr(ret.ErrCode),ret)
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

	CompareAndSet(c)

	//eventQueue.Run()

	sigStop := make(chan bool)
	_, _ = <-sigStop
}