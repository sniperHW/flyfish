package main

import(
	"flyfish"
	"fmt"
	"github.com/sniperHW/kendynet/golog"
	_ "net/http/pprof"
	"net/http"
	"github.com/sniperHW/kendynet"
	//"runtime/pprof"
	"os"
	"os/signal"
	"syscall"
)


func main() {
	//golog.DisableStdOut()
	outLogger := golog.NewOutputLogger("log", "flyfish", 1024*1024*50)
	flyfish.InitLogger(outLogger,golog.Level_Info)
	kendynet.InitLogger(outLogger,"flyfish")

	/*f, _ := os.Create("profile_file")
	pprof.StartCPUProfile(f)  // 开始cpu profile，结果写到文件f中
	defer pprof.StopCPUProfile() // 结束profile
	*/

	metas := []string{
		"users1@age:int,phone:string,name:string",
	}

	flyfish.InitMeta(metas)
	flyfish.RedisInit("127.0.0.1:6379","")
	flyfish.SQLInit("test","sniper","802802")

	go func() {
    	http.ListenAndServe("0.0.0.0:8899", nil)
	}()

	err := flyfish.StartTcpServer("tcp","localhost:10012")
	if nil == err {
		fmt.Println("flyfish start ok")
		c := make(chan os.Signal) 
		signal.Notify(c, syscall.SIGINT) //监听指定信号
		_ = <-c //阻塞直至有信号传入
		flyfish.Stop()
	}else{
		fmt.Println(err)
	}
}