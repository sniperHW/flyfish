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
	incrFlag := flag.Bool("incr-flag", true, "whether incr field value")

	flag.Parse()

	var (
		table      string
		key        string
		version    []int64
		fieldName  string
		fieldValue int64
		argi       = 0
		err        error
		cmd        *client.SliceCmd
		cli        *client.Client
	)

	args := flag.Args()
	nArgs := len(args)

	if len(args) < 4 {
		_, _ = fmt.Fprintln(os.Stderr, "args not enough")
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
			version = []int64{ver}
		}

		argi++
	}

	if nArgs-argi < 2 {
		_, _ = fmt.Fprintln(os.Stderr, "no field or value")
		os.Exit(1)
	}

	fieldName = args[argi]
	argi++
	fieldValue, err = strconv.ParseInt(args[argi], 10, 64)
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, "invalid value")
		os.Exit(1)
	}

	cli = client.OpenClient(*serverAddr, *compressFlag)

	if *incrFlag {
		cmd = cli.IncrBy(table, key, fieldName, fieldValue, version...)
	} else {
		cmd = cli.DecrBy(table, key, fieldName, fieldValue, version...)
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
}
