package login

import (
	"encoding/binary"
	"github.com/gogo/protobuf/proto"
	"github.com/sniperHW/flyfish/core/buffer"
	protocol "github.com/sniperHW/flyfish/proto"
	"net"
	"time"
)

const (
	timeout time.Duration = time.Second * 5
)

func SendLoginReq(conn *net.TCPConn, loginReq *protocol.LoginReq) bool {
	b := buffer.Get()
	defer b.Free()
	data, _ := proto.Marshal(loginReq)
	b.AppendUint16(uint16(len(data)))
	b.AppendBytes(data)

	conn.SetWriteDeadline(time.Now().Add(timeout))
	_, err := conn.Write(b.Bytes())
	conn.SetWriteDeadline(time.Time{})
	return nil == err
}

func SendLoginResp(conn *net.TCPConn, loginResp *protocol.LoginResp) bool {
	b := buffer.Get()
	defer b.Free()
	data, _ := proto.Marshal(loginResp)
	b.AppendUint16(uint16(len(data)))
	b.AppendBytes(data)

	conn.SetWriteDeadline(time.Now().Add(timeout))
	_, err := conn.Write(b.Bytes())
	conn.SetWriteDeadline(time.Time{})
	return nil == err
}

func RecvLoginReq(conn *net.TCPConn) (*protocol.LoginReq, error) {
	b := make([]byte, 1024)
	w := 0
	pbsize := 0
	for {
		conn.SetReadDeadline(time.Now().Add(timeout))
		n, err := conn.Read(b[w:])
		conn.SetReadDeadline(time.Time{})

		if nil != err {
			return nil, err
		}

		w = w + n

		if w >= 2 {
			pbsize = int(binary.BigEndian.Uint16(b[:2]))
		}

		if w >= pbsize+2 {
			loginReq := &protocol.LoginReq{}
			if err = proto.Unmarshal(b[2:w], loginReq); err == nil {
				return loginReq, nil
			} else {
				return nil, err
			}
		}
	}
}

func RecvLoginResp(conn *net.TCPConn) (*protocol.LoginResp, error) {
	b := make([]byte, 1024)
	w := 0
	pbsize := 0
	for {
		conn.SetReadDeadline(time.Now().Add(time.Second * 5))
		n, err := conn.Read(b[w:])
		conn.SetReadDeadline(time.Time{})

		if nil != err {
			return nil, err
		}

		w = w + n

		if w >= 2 {
			pbsize = int(binary.BigEndian.Uint16(b[:2]))
		}

		if w >= pbsize+2 {
			loginResp := &protocol.LoginResp{}
			if err = proto.Unmarshal(b[2:w], loginResp); err == nil {
				return loginResp, nil
			} else {
				return nil, err
			}
		}
	}
}
