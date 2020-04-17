package net

import (
	protocol "github.com/sniperHW/flyfish/proto"
	"github.com/sniperHW/flyfish/proto/login"
	"github.com/sniperHW/kendynet"
	"net"
	"sync/atomic"
)

type Listener struct {
	l           *net.TCPListener
	started     int32
	closed      int32
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
	if atomic.CompareAndSwapInt32(&this.closed, 0, 1) {
		if nil != this.l {
			this.l.Close()
		}
	}
}

func (this *Listener) Serve(onNewClient func(kendynet.StreamSession, bool)) error {

	if nil == onNewClient {
		return kendynet.ErrInvaildNewClientCB
	}

	if !atomic.CompareAndSwapInt32(&this.started, 0, 1) {
		return kendynet.ErrServerStarted
	}

	for {
		conn, err := this.l.Accept()
		if err != nil {
			if atomic.LoadInt32(&this.closed) == 1 {
				return nil
			}

			if ne, ok := err.(net.Error); ok && ne.Temporary() {
				kendynet.GetLogger().Errorf("accept temp err: %v", ne)
				continue
			} else {
				return err
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
					Ok:       true,
					Compress: loginReq.GetCompress(),
				}

				if !login.SendLoginResp(conn.(*net.TCPConn), loginResp) {
					conn.Close()
					return
				}

				onNewClient(createSession(conn), loginReq.GetCompress())

			}()
		}
	}
}
