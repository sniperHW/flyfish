package net

import (
	"net"
	"time"
)

type Udp struct {
	address string
	addr    *net.UDPAddr
	conn    *net.UDPConn
	unpack  func(from *net.UDPAddr, b []byte) (msg interface{}, err error)
	pack    func(conn *net.UDPConn, msg interface{}) ([]byte, error)
}

func NewUdp(service string, pack func(*net.UDPConn, interface{}) ([]byte, error), unpack func(*net.UDPAddr, []byte) (interface{}, error)) (*Udp, error) {
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

func (u *Udp) SetReadDeadline(t time.Time) error {
	return u.conn.SetReadDeadline(t)
}

func (u *Udp) SendTo(addr *net.UDPAddr, msg interface{}) error {
	if b, err := u.pack(u.conn, msg); nil != err {
		return err
	} else {
		_, err := u.conn.WriteToUDP(b, addr)
		return err
	}
}

func (u *Udp) ReadFrom(recvBuff []byte) (*net.UDPAddr, interface{}, error) {
	n, addr, err := u.conn.ReadFromUDP(recvBuff)
	if nil != err {
		return addr, nil, err
	}

	msg, err := u.unpack(addr, recvBuff[:n])
	return addr, msg, err
}

func (u *Udp) Close() {
	u.conn.Close()
}
