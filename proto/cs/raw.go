package cs

import (
	"encoding/binary"
	"errors"
	"github.com/gogo/protobuf/proto"
	"github.com/sniperHW/flyfish/pkg/buffer"
	"net"
	"time"
)

func Send(conn net.Conn, msg proto.Message, deadline time.Time) error {
	b := buffer.Get()
	defer b.Free()
	data, _ := proto.Marshal(msg)
	b.AppendUint32(uint32(len(data)))
	b.AppendBytes(data)

	conn.SetWriteDeadline(deadline)
	_, err := conn.Write(b.Bytes())
	conn.SetWriteDeadline(time.Time{})
	return err
}

func Recv(conn net.Conn, maxbuff int, msg proto.Message, deadline time.Time) error {
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

		if w >= 4 {
			pbsize = int(binary.BigEndian.Uint32(b[:4]))
		}

		if pbsize > len(b)-4 {
			return errors.New("invaild packet")
		}

		if w >= pbsize+4 {
			if err = proto.Unmarshal(b[4:w], msg); err == nil {
				return nil
			} else {
				return err
			}
		}
	}
}
