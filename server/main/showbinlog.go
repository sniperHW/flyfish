package main

import (
	flyfish "github.com/sniperHW/flyfish/server"
	"os"
)

func main() {
	showDetail := false
	if len(os.Args) > 2 && os.Args[2] == "detail" {
		showDetail = true
	}

	flyfish.ShowBinlog(os.Args[1], showDetail)
}
