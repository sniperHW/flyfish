package main

import (
	"flag"
	"fmt"
	"github.com/sniperHW/flyfish/client"
	"github.com/sniperHW/flyfish/errcode"
)

func main() {
	serverAddr := flag.String("server-addr", "localhost:10012", "server address")
	compressFlag := flag.Bool("compress", false, "whether compress data")

	flag.Parse()

	var (
		table   = "user_module_data"
		key     = "4540217385795583"
		version = []int64{12}
		field   = "rankdata"
		old     = []byte{1}
		new     = []byte{0}
	)

	client := client.OpenClient(*serverAddr, *compressFlag)

	result := client.CompareAndSet(table, key, field, old, new, version...).Exec()
	if result.ErrCode == errcode.ERR_OK {
		fmt.Println("compare-set successfully")
		fmt.Printf("\tversion=%d\n", result.Version)
		fmt.Printf("\t%s=%v\n", field, result.Fields[field].GetValue())
	} else {
		fmt.Printf("compare-set failed, err: %s.\n", errcode.GetErrorStr(result.ErrCode))
	}
}
