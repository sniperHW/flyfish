package cs

import (
	"fmt"
	flynet "github.com/sniperHW/flyfish/pkg/net"
	protocol "github.com/sniperHW/flyfish/proto"
	"github.com/sniperHW/flyfish/proto/login"
	"net"
	"time"
)

type Connector struct {
	nettype         string
	addr            string
	maxSendBuffSize int
}

func NewConnector(nettype string, addr string, maxSendBuffSize int) *Connector {
	return &Connector{nettype: nettype, addr: addr, maxSendBuffSize: maxSendBuffSize}
}

func (this *Connector) Dial(timeout time.Duration) (*flynet.Socket, error) {
	dialer := &net.Dialer{Timeout: timeout}
	conn, err := dialer.Dial(this.nettype, this.addr)
	if err != nil {
		return nil, err
	}

	if !login.SendLoginReq(conn.(*net.TCPConn), &protocol.LoginReq{}) {
		conn.Close()
		return nil, fmt.Errorf("login failed")
	}

	loginResp, err := login.RecvLoginResp(conn.(*net.TCPConn))
	if nil != err || !loginResp.GetOk() {
		conn.Close()
		return nil, fmt.Errorf("login failed")
	}

	return flynet.NewSocket(conn, this.maxSendBuffSize), nil
}
