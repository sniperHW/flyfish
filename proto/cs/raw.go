package cs

import (
	"encoding/binary"
	"errors"
	"github.com/gogo/protobuf/proto"
	"github.com/sniperHW/flyfish/pkg/buffer"
	"net"
	"time"
)

const maxpacket_size int = 1024 * 1024 * 100

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

func Recv(conn net.Conn, msg proto.Message, deadline time.Time) error {
	b := make([]byte, 64)
	w := 0
	pbsize := 0
	conn.SetReadDeadline(deadline)
	defer conn.SetReadDeadline(time.Time{})
	for {
		n, err := conn.Read(b[w:])
		if nil != err {
			return err
		}

		w = w + n

		if w >= 4 {
			pbsize = int(binary.BigEndian.Uint32(b[:4]))
		}

		if pbsize > maxpacket_size {
			return errors.New("packet too large")
		}

		if pbsize+4 > len(b) {
			bb := make([]byte, pbsize+4)
			copy(bb, b[:n])
			b = bb
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
