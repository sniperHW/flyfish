#!/bin/sh

go test -coverpkg=github.com/sniperHW/flyfish/server/flygate,github.com/sniperHW/flyfish/server/flypd,github.com/sniperHW/flyfish/server/flykv,github.com/sniperHW/flyfish/client -covermode=count -v -coverprofile=coverage.out -run=.