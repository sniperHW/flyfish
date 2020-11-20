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
		table = "test_indecr"
		key   = "12345555"
		//version = []int64{2}
		fields = map[string]interface{}{
			//"rankdata": make([]byte, 0),
			"n": int64(0),
		}
	)

	client := client.OpenClient(*serverAddr, *compressFlag)

	result := client.SetNx(table, key, fields).Exec()
	if result.ErrCode == errcode.ERR_OK {
		fmt.Printf("set successfully, version=%d.\n", result.Version)
	} else if result.ErrCode == errcode.ERR_RECORD_EXIST {
		fmt.Println("record exist---------------------")
		fmt.Printf("\tverison = %d\n", result.Version)
		for k, v := range result.Fields {
			fmt.Printf("\t%s = %v\n", k, v.GetValue())
		}
	} else {
		fmt.Printf("set failed, err: %s.\n", errcode.GetErrorStr(result.ErrCode))
	}
}
