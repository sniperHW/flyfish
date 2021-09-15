package net

import (
	"github.com/gogo/protobuf/proto"
	"net"
)

type Udp struct {
	address string
	addr    *net.UDPAddr
	conn    *net.UDPConn
	unpack  func(b []byte) (msg proto.Message, err error)
	pack    func(msg proto.Message) ([]byte, error)
}

func NewUdp(service string, pack func(proto.Message) ([]byte, error), unpack func([]byte) (proto.Message, error)) (*Udp, error) {
	addr, err := net.ResolveUDPAddr("udp", service)
	if nil != err {
		return nil, err
	}

	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		return nil, err
	}

	return &Udp{
		address: service,
		addr:    addr,
		conn:    conn,
		pack:    pack,
		unpack:  unpack,
	}, nil
}

func (u *Udp) SendTo(addr *net.UDPAddr, msg proto.Message) error {
	if b, err := u.pack(msg); nil != err {
		return err
	} else {
		_, err := u.conn.WriteToUDP(b, addr)
		return err
	}
}

func (u *Udp) ReadFrom(recvBuff []byte) (*net.UDPAddr, proto.Message, error) {
	n, addr, err := u.conn.ReadFromUDP(recvBuff)
	if nil != err {
		return addr, nil, err
	}

	msg, err := u.unpack(recvBuff[:n])
	return addr, msg, err
}

func (u *Udp) Close() {
	u.conn.Close()
}
