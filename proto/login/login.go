package login

import (
	"encoding/binary"
	"errors"
	"github.com/gogo/protobuf/proto"
	"github.com/sniperHW/flyfish/pkg/buffer"
	protocol "github.com/sniperHW/flyfish/proto"
	"net"
	"time"
)

func send(conn net.Conn, msg proto.Message, deadline time.Time) bool {
	b := buffer.Get()
	defer b.Free()
	data, _ := proto.Marshal(msg)
	b.AppendUint16(uint16(len(data)))
	b.AppendBytes(data)

	conn.SetWriteDeadline(deadline)
	_, err := conn.Write(b.Bytes())
	conn.SetWriteDeadline(time.Time{})
	return nil == err
}

func SendLoginReq(conn net.Conn, loginReq *protocol.LoginReq, deadline time.Time) bool {
	return send(conn, loginReq, deadline)
}

func SendLoginResp(conn net.Conn, loginResp *protocol.LoginResp, deadline time.Time) bool {
	return send(conn, loginResp, deadline)
}

func recv(conn net.Conn, maxbuff int, msg proto.Message, deadline time.Time) error {
	b := make([]byte, maxbuff)
	w := 0
	pbsize := 0
	for {
		conn.SetReadDeadline(deadline)
		n, err := conn.Read(b[w:])
		conn.SetReadDeadline(time.Time{})

		if nil != err {
			return err
		}

		w = w + n

		if w >= 2 {
			pbsize = int(binary.BigEndian.Uint16(b[:2]))
		}

		if pbsize > len(b)-2 {
			return errors.New("invaild packet")
		}

		if w >= pbsize+2 {
			if err = proto.Unmarshal(b[2:w], msg); err == nil {
				return nil
			} else {
				return err
			}
		}
	}
}

func RecvLoginReq(conn net.Conn, deadline time.Time) (*protocol.LoginReq, error) {
	loginReq := &protocol.LoginReq{}
	err := recv(conn, 128, loginReq, deadline)
	return loginReq, err
}

func RecvLoginResp(conn net.Conn, deadline time.Time) (*protocol.LoginResp, error) {
	loginResp := &protocol.LoginResp{}
	err := recv(conn, 128, loginResp, deadline)
	return loginResp, err
}
