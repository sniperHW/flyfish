package scanner

import (
	//"errors"
	"github.com/sniperHW/flyfish/client"
	//flyproto "github.com/sniperHW/flyfish/proto"
	//"github.com/sniperHW/flyfish/proto/cs/scan"
	//"math/rand"
	"net"
	"time"
)

type scannerFlysql struct {
	impl
}

func queryFlysql(pdAddr []*net.UDPAddr, deadline time.Time) []string {
	return client.QueryFlysql(pdAddr, deadline.Sub(time.Now()))
}
