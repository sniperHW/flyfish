package cs

import (
	flynet "github.com/sniperHW/flyfish/pkg/net"
	protocol "github.com/sniperHW/flyfish/proto"
	"github.com/sniperHW/flyfish/proto/login"
	"net"
	"sync"
)

type Listener struct {
	l           *net.TCPListener
	startOnce   sync.Once
	closeOnce   sync.Once
	verifyLogin func(*protocol.LoginReq) bool
}

func NewListener(nettype, service string, verifyLogin func(*protocol.LoginReq) bool) (*Listener, error) {
	tcpAddr, err := net.ResolveTCPAddr(nettype, service)
	if err != nil {
		return nil, err
	}

	l, err := net.ListenTCP(nettype, tcpAddr)
	if err != nil {
		return nil, err
	}
	return &Listener{l: l, verifyLogin: verifyLogin}, nil
}

func (this *Listener) Close() {
	this.closeOnce.Do(func() {
		if nil != this.l {
			this.l.Close()
		}
	})
}

func (this *Listener) Serve(onNewClient func(*flynet.Socket)) {

	this.startOnce.Do(func() {
		go func() {
			for {
				conn, err := this.l.Accept()
				if err != nil {
					if ne, ok := err.(net.Error); ok && ne.Temporary() {
						continue
					} else {
						return
					}

				} else {
					go func() {
						loginReq, err := login.RecvLoginReq(conn.(*net.TCPConn))
						if nil != err {
							conn.Close()
							return
						}

						if !this.verifyLogin(loginReq) {
							conn.Close()
							return
						}

						loginResp := &protocol.LoginResp{
							Ok: true,
						}

						if !login.SendLoginResp(conn.(*net.TCPConn), loginResp) {
							conn.Close()
							return
						}

						onNewClient(flynet.CreateSocket(conn))
					}()
				}
			}
		}()
	})
}
