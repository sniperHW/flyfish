package main 

import(
	"fmt"
	kclient "flyfish/client"
	"flyfish/errcode"
	"github.com/sniperHW/kendynet/golog"
	"github.com/sniperHW/kendynet"
)

func CompareAndSetNx(c *kclient.Client) {
	set := c.CompareAndSetNx("counter","test_counter3","c",10,10)
	set.Exec(func(ret *kclient.SliceResult) {

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

	services := []string{"127.0.0.1:10012"}
	c := kclient.OpenClient(services)//eventQueue)

	CompareAndSetNx(c)

	//eventQueue.Run()

	sigStop := make(chan bool)
	_, _ = <-sigStop
}