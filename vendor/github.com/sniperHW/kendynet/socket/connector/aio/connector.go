package tcp

import (
	"github.com/sniperHW/kendynet"
	"github.com/sniperHW/kendynet/socket/aio"
	"net"
	"time"
)

type Connector struct {
	nettype string
	addr    string
	s       *aio.AioService
}

func New(s *aio.AioService, nettype string, addr string) (*Connector, error) {
	return &Connector{s: s, nettype: nettype, addr: addr}, nil
}

func (this *Connector) Dial(timeout time.Duration) (kendynet.StreamSession, error) {
	dialer := &net.Dialer{Timeout: timeout}
	conn, err := dialer.Dial(this.nettype, this.addr)
	if err != nil {
		return nil, err
	}
	return aio.NewAioSocket(this.s, conn), nil
}
