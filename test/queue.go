package main 

import(
	"github.com/sniperHW/kendynet/util"
	"time"
	"fmt"
)

func main() {

	queue := util.NewBlockQueue(1000)

	go func(){
		for {
			queue.Add(1)
		}
	}()

	go func(){
		for {
			queue.Add(1)
		}
	}()


	go func(){
		c := 0
		for {
			time.Sleep(time.Second)
			closed, list := queue.Get()
			c += len(list)
			fmt.Println(c)
			if closed {
				return
			}
		}
	}()

	sigStop := make(chan bool)
	_ , _ = <-sigStop

}