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

type scannerFlygate struct {
	impl
}

func queryFlygate(pdAddr []*net.UDPAddr, deadline time.Time) []string {
	return client.QueryGate(pdAddr, deadline.Sub(time.Now()))
}
