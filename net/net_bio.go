// +build !aio

package net

import (
	"fmt"
	//"github.com/golang/protobuf/proto"
	"github.com/gogo/protobuf/proto"
	"github.com/sniperHW/flyfish/conf"
	"github.com/sniperHW/flyfish/net/pb"
	"github.com/sniperHW/kendynet"
	"github.com/sniperHW/kendynet/socket"
	"net"
)

func createSession(conn net.Conn) kendynet.StreamSession {
	return socket.NewStreamSocket(conn)
}

type Receiver struct {
	buffer         []byte
	w              uint64
	r              uint64
	nextPacketSize uint64
	unCompressor   UnCompressorI
	pbSpace        *pb.Namespace
}

func NewReceiver(pbSpace *pb.Namespace, compress bool) *Receiver {
	receiver := &Receiver{
		pbSpace: pbSpace,
	}
	receiver.buffer = make([]byte, initBufferSize)
	if compress {
		receiver.unCompressor = &ZipUnCompressor{}
	}
	return receiver
}

func (this *Receiver) unPack() (ret interface{}, err error) {
	unpackSize := uint64(this.w - this.r)
	if unpackSize > minSize {
		var payload uint32
		var cmd uint16
		var buff []byte
		var msg proto.Message
		var totalSize uint64
		var flag byte

		reader := kendynet.NewReader(kendynet.NewByteBuffer(this.buffer[this.r:], unpackSize))
		if payload, err = reader.GetUint32(); err != nil {
			return
		}

		if uint64(payload) == 0 {
			err = fmt.Errorf("zero payload")
			return
		}

		if uint64(payload)+SizeLen > conf.MaxPacketSize {
			err = fmt.Errorf("large packet %d", uint64(payload)+SizeLen)
			return
		}

		totalSize = uint64(payload + SizeLen)

		this.nextPacketSize = totalSize

		if totalSize <= unpackSize {

			if flag, err = reader.GetByte(); err != nil {
				return
			}

			//read head
			var head CommonHead
			var sizeOfUniKey int16

			if head.Seqno, err = reader.GetInt64(); err != nil {
				return
			}

			if head.ErrCode, err = reader.GetInt32(); err != nil {
				return
			}

			if head.Timeout, err = reader.GetUint32(); err != nil {
				return
			}

			if sizeOfUniKey, err = reader.GetInt16(); err != nil {
				return
			}

			if sizeOfUniKey > 0 {
				if head.UniKey, err = reader.GetString(uint64(sizeOfUniKey)); err != nil {
					return
				}
			}

			if cmd, err = reader.GetUint16(); err != nil {
				return
			}
			sizeOfHead := 8 + 4 + 4 + 2 + uint32(sizeOfUniKey)
			//普通消息
			size := payload - SizeCmd - SizeFlag - sizeOfHead
			if buff, err = reader.GetBytes(uint64(size)); err != nil {
				return
			}

			if flag == byte(1) {
				if nil == this.unCompressor {
					err = fmt.Errorf("invaild compress packet")
					return
				}

				if buff, err = this.unCompressor.UnCompress(buff); err != nil {
					return
				}
			}

			if msg, err = this.pbSpace.Unmarshal(uint32(cmd), buff); err != nil {
				return
			}
			this.nextPacketSize = 0
			this.r += totalSize
			ret = NewMessageWithCmd(uint16(cmd), head, msg)
		}
	}
	return
}

func (this *Receiver) ReceiveAndUnpack(sess kendynet.StreamSession) (interface{}, error) {
	var msg interface{}
	var err error
	for {
		msg, err = this.unPack()
		if nil != msg {
			return msg, nil
		} else if err == nil {
			if this.w == this.r {
				this.w = 0
				this.r = 0
			} else {
				if this.nextPacketSize > uint64(cap(this.buffer)) {
					buffer := make([]byte, sizeofPow2(this.nextPacketSize))
					copy(buffer, this.buffer[this.r:this.w])
					this.buffer = buffer
				} else {
					//空间足够容纳下一个包，
					copy(this.buffer, this.buffer[this.r:this.w])
				}
				this.w = this.w - this.r
				this.r = 0
			}

			n, err := sess.(*socket.StreamSocket).Read(this.buffer[this.w:])

			if n > 0 {
				this.w += uint64(n) //增加待解包数据
			}
			if err != nil {
				return nil, err
			}
		} else {
			return nil, err
		}
	}
}

func (this *Receiver) GetRecvBuff() []byte {
	return nil
}

func (this *Receiver) Unpack() (interface{}, error) {
	return nil, nil
}

func (this *Receiver) OnData(buff []byte) {
}

func (this *Receiver) OnSocketClose() {

}
