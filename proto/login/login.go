package login

import (
	"encoding/binary"
	//"github.com/golang/protobuf/proto"
	"github.com/gogo/protobuf/proto"
	protocol "github.com/sniperHW/flyfish/proto"
	"github.com/sniperHW/kendynet"
	"net"
	"time"
)

const (
	timeout time.Duration = time.Second * 5
)

func SendLoginReq(conn *net.TCPConn, loginReq *protocol.LoginReq) bool {
	buffer := kendynet.NewByteBuffer(64)
	data, _ := proto.Marshal(loginReq)
	buffer.AppendUint16(uint16(len(data)))
	buffer.AppendBytes(data)

	conn.SetWriteDeadline(time.Now().Add(timeout))
	_, err := conn.Write(buffer.Bytes())
	conn.SetWriteDeadline(time.Time{})
	return nil == err
}

func SendLoginResp(conn *net.TCPConn, loginResp *protocol.LoginResp) bool {
	buffer := kendynet.NewByteBuffer(64)
	data, _ := proto.Marshal(loginResp)
	buffer.AppendUint16(uint16(len(data)))
	buffer.AppendBytes(data)

	conn.SetWriteDeadline(time.Now().Add(timeout))
	_, err := conn.Write(buffer.Bytes())
	conn.SetWriteDeadline(time.Time{})
	return nil == err
}

func RecvLoginReq(conn *net.TCPConn) (*protocol.LoginReq, error) {
	buffer := make([]byte, 1024)
	w := 0
	pbsize := 0
	for {
		conn.SetReadDeadline(time.Now().Add(timeout))
		n, err := conn.Read(buffer[w:])
		conn.SetReadDeadline(time.Time{})

		if nil != err {
			return nil, err
		}

		w = w + n

		if w >= 2 {
			pbsize = int(binary.BigEndian.Uint16(buffer[:2]))
		}

		if w >= pbsize+2 {
			loginReq := &protocol.LoginReq{}
			if err = proto.Unmarshal(buffer[2:w], loginReq); err == nil {
				return loginReq, nil
			} else {
				return nil, err
			}
		}
	}
}

func RecvLoginResp(conn *net.TCPConn) (*protocol.LoginResp, error) {
	buffer := make([]byte, 1024)
	w := 0
	pbsize := 0
	for {
		conn.SetReadDeadline(time.Now().Add(time.Second * 5))
		n, err := conn.Read(buffer[w:])
		conn.SetReadDeadline(time.Time{})

		if nil != err {
			return nil, err
		}

		w = w + n

		if w >= 2 {
			pbsize = int(binary.BigEndian.Uint16(buffer[:2]))
		}

		if w >= pbsize+2 {
			loginResp := &protocol.LoginResp{}
			if err = proto.Unmarshal(buffer[2:w], loginResp); err == nil {
				return loginResp, nil
			} else {
				return nil, err
			}
		}
	}
}
