package udp

import (
	"fmt"
	"github.com/gogo/protobuf/proto"
	snet "github.com/sniperHW/flyfish/server/net"
	"math/rand"
	"time"
)

func Call(remotes interface{}, req proto.Message, timeout time.Duration) (proto.Message, error) {
	randNum := rand.Int63()
	resp := snet.UdpCall(remotes,
		snet.MakeMessage(randNum, req),
		timeout,
		func(respCh chan interface{}, r interface{}) {
			if m, ok := r.(*snet.Message); ok {
				select {
				case respCh <- m:
				default:
				}
			}
		})

	if nil == resp {
		return nil, fmt.Errorf("timeout")
	} else if randNum != resp.(*snet.Message).Context {
		return nil, fmt.Errorf("invalid response")
	} else {
		if r, ok := resp.(*snet.Message).Msg.(proto.Message); ok {
			return r, nil
		} else {
			return nil, fmt.Errorf("invalid response")
		}
	}
}
