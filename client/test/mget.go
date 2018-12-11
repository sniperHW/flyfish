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

	mget.Exec(func(ret *kclient.ResultSet){
		if ret.ErrCode == errcode.ERR_OK {
			for k,v := range(ret.Results) {
				if nil != v {
					fmt.Println("age",v.Fields["age"].GetInt())
				} else {
					fmt.Println(k,"not exist")
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

	c := kclient.OpenClient("localhost:10012")//eventQueue)

	MGet(c)

	//eventQueue.Run()

	sigStop := make(chan bool)
	_, _ = <-sigStop
}