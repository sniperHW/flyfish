package net

import (
	"net"
	"sync"
	"sync/atomic"
)

const (
	fclosed  = int32(1 << 1)
	frclosed = int32(1 << 2)
	fwclosed = int32(1 << 3)
	fdoclose = int32(1 << 4)
)

type socketBase struct {
	conn             net.Conn
	flag             int32
	ud               atomic.Value
	sendCloseChan    chan struct{}
	closeOnce        sync.Once
	beginOnce        sync.Once
	sendOnce         sync.Once
	encoder          Encoder
	inboundProcessor InBoundProcessor
	errorCallback    func(*Socket, error)
	closeCallBack    func(*Socket, error)
	inboundCallBack  func(*Socket, interface{})
	ioCount          int32
	closeReason      error
}

func (s *Socket) setFlag(flag int32) {
	for {
		f := atomic.LoadInt32(&s.flag)
		if atomic.CompareAndSwapInt32(&s.flag, f, f|flag) {
			break
		}
	}
}

func (s *Socket) testFlag(flag int32) bool {
	return atomic.LoadInt32(&s.flag)&flag > 0
}

func (s *Socket) IsClosed() bool {
	return s.testFlag(fclosed)
}

func (s *Socket) LocalAddr() net.Addr {
	return s.conn.LocalAddr()
}

func (s *Socket) RemoteAddr() net.Addr {
	return s.conn.RemoteAddr()
}

func (s *Socket) SetUserData(ud interface{}) *Socket {
	s.ud.Store(ud)
	return s
}

func (s *Socket) GetUserData() interface{} {
	return s.ud.Load()
}

func (s *Socket) GetNetConn() net.Conn {
	return s.conn
}

func (s *Socket) SetErrorCallBack(cb func(*Socket, error)) *Socket {
	s.errorCallback = cb
	return s
}

func (s *Socket) SetCloseCallBack(cb func(*Socket, error)) *Socket {
	s.closeCallBack = cb
	return s
}

func (s *Socket) SetEncoder(encoder Encoder) *Socket {
	s.encoder = encoder
	return s
}

func (s *Socket) SetInBoundProcessor(in InBoundProcessor) *Socket {
	s.inboundProcessor = in
	return s
}

func (s *Socket) ShutdownRead() {
	s.setFlag(frclosed)
	s.conn.(interface{ CloseRead() error }).CloseRead()
}

func (s *Socket) addIO() {
	atomic.AddInt32(&s.ioCount, 1)
}