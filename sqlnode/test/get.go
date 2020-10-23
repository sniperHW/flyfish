package main

import (
	"flag"
	"fmt"
	"github.com/sniperHW/flyfish/client"
	"github.com/sniperHW/flyfish/errcode"
	"os"
	"strconv"
)

func main() {
	serverAddr := flag.String("server-addr", "localhost:10012", "server address")
	compressFlag := flag.Bool("compress", false, "whether compress data")
	versionFlag := flag.Bool("with-version", false, "whether get with version")
	getAllField := flag.Bool("all-field", false, "whether get all field")

	flag.Parse()

	var table string
	var key string
	var version int64
	var fields []string
	var cmd *client.SliceCmd
	var argi = 0

	args := flag.Args()
	nArgs := len(args)

	if len(args) < 2 {
		_, _ = fmt.Fprintln(os.Stderr, "no table or key")
		os.Exit(1)
	}

	table = args[argi]
	argi++

	key = args[argi]
	argi++

	if *versionFlag {
		if argi >= nArgs {
			_, _ = fmt.Fprintln(os.Stderr, "no version")
			os.Exit(1)
		} else if ver, err := strconv.ParseInt(args[argi], 10, 64); err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "invalid version: %s\n", args[argi])
			os.Exit(1)
		} else {
			version = ver
		}

		argi++
	}

	if !*getAllField {
		if argi >= len(args) {
			_, _ = fmt.Fprintln(os.Stderr, "no field")
			os.Exit(1)
		}

		fields = args[argi:]
	}

	client := client.OpenClient(*serverAddr, *compressFlag)

	if *getAllField {
		if *versionFlag {
			cmd = client.GetAllWithVersion(table, key, version)
		} else {
			cmd = client.GetAll(table, key)
		}
	} else {
		if *versionFlag {
			cmd = client.GetWithVersion(table, key, version, fields...)
		} else {
			cmd = client.Get(table, key, fields...)
		}
	}

	result := cmd.Exec()
	if result.ErrCode == errcode.ERR_OK {
		fmt.Println("result-----------------------------")
		fmt.Printf("\tversion = %d\n", result.Version)
		for k, v := range result.Fields {
			fmt.Printf("\t%s = %v\n", k, v.GetValue())
		}
	} else {
		fmt.Println(errcode.GetErrorStr(result.ErrCode))
	}

	fmt.Println("\ndone.")
}
