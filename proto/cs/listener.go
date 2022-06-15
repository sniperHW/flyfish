package cs

import (
	"github.com/sniperHW/flyfish/logger"
	Crypto "github.com/sniperHW/flyfish/pkg/crypto"
	flynet "github.com/sniperHW/flyfish/pkg/net"
	protocol "github.com/sniperHW/flyfish/proto"
	"net"
	"sync/atomic"
	"time"
)

func SendLoginResp(conn net.Conn, loginResp *protocol.LoginResp, deadline time.Time) error {
	return Send(conn, loginResp, deadline, true)
}

func RecvLoginReq(conn net.Conn, deadline time.Time) (*protocol.LoginReq, error) {
	loginReq := &protocol.LoginReq{}
	err := Recv(conn, loginReq, deadline, true)
	return loginReq, err
}

type Listener struct {
	l              *net.TCPListener
	startOnce      int32
	closeOnce      int32
	outputBufLimit flynet.OutputBufLimit
}

func NewListener(nettype, service string, outputBufLimit flynet.OutputBufLimit) (*Listener, error) {
	tcpAddr, err := net.ResolveTCPAddr(nettype, service)
	if err != nil {
		return nil, err
	}

	l, err := net.ListenTCP(nettype, tcpAddr)
	if err != nil {
		return nil, err
	}
	return &Listener{l: l, outputBufLimit: outputBufLimit}, nil
}

func (this *Listener) Close() {
	if atomic.CompareAndSwapInt32(&this.closeOnce, 0, 1) {
		if nil != this.l {
			this.l.Close()
		}
	}
}

func (this *Listener) Serve(onNewClient func(*flynet.Socket), onScanner ...func(net.Conn)) {

	var onscanner func(net.Conn)

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

						loginReq, err := RecvLoginReq(conn, deadline)
						if nil != err {
							conn.Close()
							return
						}

						key, err := Crypto.AESCBCDecrypter(cipherbyte, loginReq.Key)

						if nil != err {
							logger.GetSugar().Errorf("AESCBCDecrypter error:%v", err)
							conn.Close()
							return
						} else if string(key) != conn.RemoteAddr().String() {
							logger.GetSugar().Errorf("invaild client Key(%s):RemoteAddr(%s)", key, conn.RemoteAddr().String())
							conn.Close()
							return
						}

						loginResp := &protocol.LoginResp{}
						if loginReq.Scanner && onscanner == nil {
							loginResp.Ok = false
							loginResp.Reason = "unsupported scanner client"
						} else {
							loginResp.Ok = true
						}

						if nil != SendLoginResp(conn, loginResp, deadline) {
							conn.Close()
							return
						}

						if loginResp.Ok {
							if loginReq.Scanner {
								onscanner(conn)
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
