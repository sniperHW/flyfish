package cs

import (
	flynet "github.com/sniperHW/flyfish/pkg/net"
	protocol "github.com/sniperHW/flyfish/proto"
	"github.com/sniperHW/flyfish/proto/login"
	"net"
	"sync/atomic"
	"time"
)

type Listener struct {
	l              *net.TCPListener
	startOnce      int32
	closeOnce      int32
	verifyLogin    func(*protocol.LoginReq) bool
	outputBufLimit flynet.OutputBufLimit
}

func NewListener(nettype, service string, outputBufLimit flynet.OutputBufLimit, verifyLogin func(*protocol.LoginReq) bool) (*Listener, error) {
	tcpAddr, err := net.ResolveTCPAddr(nettype, service)
	if err != nil {
		return nil, err
	}

	l, err := net.ListenTCP(nettype, tcpAddr)
	if err != nil {
		return nil, err
	}
	return &Listener{l: l, verifyLogin: verifyLogin, outputBufLimit: outputBufLimit}, nil
}

func (this *Listener) Close() {
	if atomic.CompareAndSwapInt32(&this.closeOnce, 0, 1) {
		if nil != this.l {
			this.l.Close()
		}
	}
}

func (this *Listener) Serve(onNewClient func(*flynet.Socket), onScanner ...func(*flynet.Socket)) {

	var onscanner func(*flynet.Socket)

	if len(onScanner) > 0 {
		onscanner = onScanner[0]
	}

	if atomic.CompareAndSwapInt32(&this.startOnce, 0, 1) {
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
						deadline := time.Now().Add(time.Second * 5)

						loginReq, err := login.RecvLoginReq(conn, deadline)
						if nil != err {
							conn.Close()
							return
						}

						loginResp := &protocol.LoginResp{}

						if !this.verifyLogin(loginReq) {
							loginResp.Ok = false
							loginResp.Reason = "verify failed"
						} else if loginReq.Scanner && onscanner == nil {
							loginResp.Ok = false
							loginResp.Reason = "unsupported scanner client"
						} else {
							loginResp.Ok = true
						}

						if !login.SendLoginResp(conn, loginResp, deadline) {
							conn.Close()
							return
						}

						if loginResp.Ok {
							if loginReq.Scanner {
								onscanner(flynet.NewSocket(conn, this.outputBufLimit))
							} else {
								onNewClient(flynet.NewSocket(conn, this.outputBufLimit))
							}
						} else {
							conn.Close()
						}
					}()
				}
			}
		}()
	}
}
