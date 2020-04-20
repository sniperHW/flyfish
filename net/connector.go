package net

import (
	"fmt"
	protocol "github.com/sniperHW/flyfish/proto"
	"github.com/sniperHW/flyfish/proto/login"
	"github.com/sniperHW/kendynet"
	"net"
	"time"
)

type Connector struct {
	nettype  string
	addr     string
	compress bool
	//createSession func(net.Conn) kendynet.StreamSession
}

func NewConnector(nettype string, addr string, compress bool) *Connector {
	return &Connector{nettype: nettype, addr: addr, compress: compress}
}

func (this *Connector) Dial(timeout time.Duration) (kendynet.StreamSession, bool, error) {
	dialer := &net.Dialer{Timeout: timeout}
	conn, err := dialer.Dial(this.nettype, this.addr)
	if err != nil {
		return nil, false, err
	}

	if !login.SendLoginReq(conn.(*net.TCPConn), &protocol.LoginReq{Compress: this.compress}) {
		conn.Close()
		return nil, false, fmt.Errorf("login failed")
	}

	loginResp, err := login.RecvLoginResp(conn.(*net.TCPConn))
	if nil != err || !loginResp.GetOk() {
		conn.Close()
		return nil, false, fmt.Errorf("login failed")
	}

	return createSession(conn), loginResp.GetCompress(), nil
}
