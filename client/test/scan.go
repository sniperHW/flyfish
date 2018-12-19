package main 

import(
	"fmt"
	kclient "flyfish/client"
	"flyfish/errcode"
	"github.com/sniperHW/kendynet/golog"
	"github.com/sniperHW/kendynet"
)

/*
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
}*/


func scanCb(scaner *kclient.Scaner, ret *kclient.MutiResult) {

	fmt.Println("scanCb",ret)

	if ret.ErrCode == errcode.ERR_OK {

		for _,v := range(ret.Rows) {
			fmt.Println(v.Key)
		}

		scaner.Close()


	} else {

		fmt.Println(errcode.GetErrorStr(ret.ErrCode))

		scaner.Close()
	}
}

func main() {

	//golog.DisableStdOut()
	outLogger := golog.NewOutputLogger("log", "flyfish get", 1024*1024*50)
	kendynet.InitLogger(outLogger,"flyfish get")

	services := []string{"127.0.0.1:10012"}
	c := kclient.OpenClient(services)//eventQueue)

	scaner := c.Scaner("users1")

	scaner.Next(10,scanCb)

	sigStop := make(chan bool)
	_, _ = <-sigStop
}