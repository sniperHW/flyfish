package main 

import(
	"fmt"
	kclient "flyfish/client"
	"flyfish/errcode"
	"github.com/sniperHW/kendynet/golog"
	"github.com/sniperHW/kendynet"
)

func MGet(c *kclient.Client) {

	keys := []string{"huangwei:1","huangwei:2","huangwei:3","huangwei:xx"}

	mget := c.MGetAll("users1",keys)

	mget.Exec(func(ret *kclient.MutiResult){
		if ret.ErrCode == errcode.ERR_OK {
			for _,v := range(ret.Rows) {
				if nil == v.Fields {
					fmt.Println(v.Key,"not exist")
				} else {
					fmt.Println("age",v.Fields["age"].GetInt())					
				}
			}
		} else {
			fmt.Println(errcode.GetErrorStr(ret.ErrCode))
		}
	})
}


func main() {

	//golog.DisableStdOut()
	outLogger := golog.NewOutputLogger("log", "flyfish get", 1024*1024*50)
	kendynet.InitLogger(outLogger,"flyfish get")

	services := []string{"127.0.0.1:10012"}
	c := kclient.OpenClient(services)//eventQueue)

	MGet(c)

	//eventQueue.Run()

	sigStop := make(chan bool)
	_, _ = <-sigStop
}