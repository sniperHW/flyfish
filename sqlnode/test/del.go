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
	var (
		serverAddr   = flag.String("server-addr", "localhost:10012", "server address")
		compressFlag = flag.Bool("compress", false, "whether compress data")
		versionFlag  = flag.Bool("with-version", false, "whether get with version")

		table   string
		key     string
		version []int64
		argi    = 0
		cmd     *client.StatusCmd
		cli     *client.Client
	)

	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "usage: %s [--server-addr addr] [--compress] [--with-version] table key [version]\n", flag.Args()[0])
		flag.CommandLine.PrintDefaults()
	}

	flag.Parse()

	args := flag.Args()
	nArgs := len(args)

	if *versionFlag && nArgs < 3 || nArgs < 2 {
		flag.Usage()
		os.Exit(1)
	}

	table = args[argi]
	argi++

	key = args[argi]
	argi++

	if *versionFlag {
		if ver, err := strconv.ParseInt(args[argi], 10, 64); err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "invalid version: %s\n", args[argi])
			os.Exit(1)
		} else {
			version = []int64{ver}
		}

		argi++
	}

	cli = client.OpenClient(*serverAddr, *compressFlag)
	cmd = cli.Del(table, key, version...)
	result := cmd.Exec()

	if result.ErrCode == errcode.ERR_OK {
		fmt.Println("del successfully")
	} else {
		fmt.Printf("del failed: %s\n", errcode.GetErrorStr(result.ErrCode))
	}
}
