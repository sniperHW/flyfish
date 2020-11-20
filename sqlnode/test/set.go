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
		key     = "4540217385795585"
		version = []int64{}
		fields  = map[string]interface{}{
			"rankdata": make([]byte, 0),
		}
	)

	client := client.OpenClient(*serverAddr, *compressFlag)

	result := client.Set(table, key, fields, version...).Exec()
	if result.ErrCode == errcode.ERR_OK {
		fmt.Printf("set successfully, version=%d.\n", result.Version)
	} else {
		fmt.Printf("set failed, err: %s.\n", errcode.GetErrorStr(result.ErrCode))
	}
}
