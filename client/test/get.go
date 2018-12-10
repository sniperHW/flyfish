package main 

import(
	"fmt"
	kclient "flyfish/client"
	"flyfish/errcode"
	"github.com/sniperHW/kendynet/golog"
	"github.com/sniperHW/kendynet"
)

func Get(c *kclient.Client) {

	key := fmt.Sprintf("%s:%d","huangwei",1)//rand.Int()%100000)
	get := c.Get("users1",key,"name","age","phone")

	get.Exec(func(ret *kclient.Result) {
		if ret.ErrCode == errcode.ERR_OK {
			fmt.Println("age",ret.Fields["age"].GetInt())
		} else {
			fmt.Println(errcode.GetErrorStr(ret.ErrCode))
		}
	})
}


func main() {

	golog.DisableStdOut()
	outLogger := golog.NewOutputLogger("log", "flyfish get", 1024*1024*50)
	kendynet.InitLogger(outLogger,"flyfish get")

	c := kclient.OpenClient("localhost:10012")//eventQueue)

	Get(c)

	//eventQueue.Run()

	sigStop := make(chan bool)
	_, _ = <-sigStop
}