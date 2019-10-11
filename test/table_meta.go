package main

import (
	"fmt"
	"github.com/sniperHW/flyfish"
)

func main() {

	strs := []string{
		"user@name:string,age:uint,phone:string,__version__:int",
		"user1@name:string,age:uint,phone:string,__version__:int",
		"user2@name:string,age:uint,phone:string,__version__:int",
		"user3@name:string,age:uint,phone:string,__version__:int",
	}

	fmt.Println(flyfish.InitMeta(strs))

}
