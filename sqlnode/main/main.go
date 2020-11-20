package main

import (
	"flag"
	"github.com/sniperHW/flyfish/sqlnode"
	"os"
	"os/signal"
)

func main() {
	config := flag.String("config", "config.toml", "config file path")

	flag.Parse()

	sqlnode.Start(*config)

	c := make(chan os.Signal)
	signal.Notify(c, os.Interrupt)
	_ = <-c
	sqlnode.Stop()
}
