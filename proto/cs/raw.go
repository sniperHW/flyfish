package cs

import (
	"encoding/binary"
	"errors"
	"github.com/gogo/protobuf/proto"
	Crypto "github.com/sniperHW/flyfish/pkg/crypto"
	"io"
	"net"
	"time"
)

const maxpacket_size int = 1024 * 1024 * 100

var cipherbyte []byte = []byte("feiyu_tech_2022")

func Send(conn net.Conn, msg proto.Message, deadline time.Time, crypto ...bool) (err error) {
	data, _ := proto.Marshal(msg)
	if len(crypto) > 0 && crypto[0] {
		if data, err = Crypto.AESCBCEncrypt(cipherbyte, data); nil != err {
			return
		}
	}
	b := make([]byte, 4+len(data))
	binary.BigEndian.PutUint32(b, uint32(len(data)))
	copy(b[4:], data)
	conn.SetWriteDeadline(deadline)
	_, err = conn.Write(b)
	conn.SetWriteDeadline(time.Time{})
	return
}

func Recv(conn net.Conn, msg proto.Message, deadline time.Time, crypto ...bool) error {
	bLen := make([]byte, 4)
	conn.SetReadDeadline(deadline)
	defer conn.SetReadDeadline(time.Time{})

	_, err := io.ReadFull(conn, bLen)
	if nil != err {
		return err
	}

	datasize := int(binary.BigEndian.Uint32(bLen))

	if datasize > maxpacket_size {
		return errors.New("packet too large")
	}

	b := make([]byte, datasize)

	_, err = io.ReadFull(conn, b)
	if nil != err {
		return err
	}

	if len(crypto) > 0 && crypto[0] {
		if b, err = Crypto.AESCBCDecrypter(cipherbyte, b); nil != err {
			return err
		}
	}

	if err = proto.Unmarshal(b, msg); err == nil {
		return nil
	} else {
		return err
	}
}
