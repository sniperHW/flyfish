package cs

import (
	"fmt"
	flynet "github.com/sniperHW/flyfish/pkg/net"
	protocol "github.com/sniperHW/flyfish/proto"
	"net"
	"time"
)

func SendLoginReq(conn net.Conn, loginReq *protocol.LoginReq, deadline time.Time) bool {
	return Send(conn, loginReq, deadline) == nil
}

func RecvLoginResp(conn net.Conn, deadline time.Time) (*protocol.LoginResp, error) {
	loginResp := &protocol.LoginResp{}
	err := Recv(conn, 128, loginResp, deadline)
	return loginResp, err
}

type Connector struct {
	nettype        string
	addr           string
	outputBufLimit flynet.OutputBufLimit
}

func NewConnector(nettype string, addr string, outputBufLimit flynet.OutputBufLimit) *Connector {
	return &Connector{nettype: nettype, addr: addr, outputBufLimit: outputBufLimit}
}

func (this *Connector) Dial(timeout time.Duration) (*flynet.Socket, error) {
	deadline := time.Now().Add(timeout)

	dialer := &net.Dialer{Timeout: timeout}
	conn, err := dialer.Dial(this.nettype, this.addr)
	if err != nil {
		return nil, err
	}

	if !SendLoginReq(conn, &protocol.LoginReq{}, deadline) {
		conn.Close()
		return nil, fmt.Errorf("login failed")
	}

	loginResp, err := RecvLoginResp(conn, deadline)
	if nil != err || !loginResp.GetOk() {
		conn.Close()
		return nil, fmt.Errorf("login failed")
	}

	return flynet.NewSocket(conn, this.outputBufLimit), nil
}
