package main 

import(
	"fmt"
	kclient "flyfish/client"
	"flyfish/errcode"
	"github.com/sniperHW/kendynet/golog"
	"github.com/sniperHW/kendynet"
)

func Get(c *kclient.Client,i int) {

	key := fmt.Sprintf("%s:%d","huangwei",i)//rand.Int()%100000)
	get := c.Get("users1",key,"name","age","phone")

	get.Exec(func(ret *kclient.SliceResult) {
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

	services := []string{"127.0.0.1:10012"}
	c := kclient.OpenClient(services)//eventQueue)

	Get(c,1)
	Get(c,2)
	Get(c,3)
	Get(c,4)

	//eventQueue.Run()

	sigStop := make(chan bool)
	_, _ = <-sigStop
}