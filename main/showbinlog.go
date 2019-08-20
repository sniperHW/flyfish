package main

import (
	"github.com/sniperHW/flyfish"
	"os"
)

func main() {
	flyfish.ShowBinlog(os.Args[1])
}
