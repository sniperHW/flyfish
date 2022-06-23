package udp

import (
	//"fmt"
	"github.com/gogo/protobuf/proto"
	snet "github.com/sniperHW/flyfish/server/net"
	//"math/rand"
	"time"
)

func Call(remotes interface{}, req proto.Message, respType proto.Message, timeout time.Duration) (resp proto.Message, err error) {
	resp, err = snet.UdpCall(remotes, req, respType, timeout)
	return resp, err
}
