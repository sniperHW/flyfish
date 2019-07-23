package codec

import (
	"flyfish/codec/pb"
	"flyfish/conf"
	"fmt"
	"net"

	"github.com/golang/protobuf/proto"
	"github.com/sniperHW/kendynet"
)

const (
	minSize uint64 = sizeLen + sizeCmd + sizeFlag
)

type Receiver struct {
	buffer       []byte
	w            uint64
	r            uint64
	unCompressor UnCompressorI
}

func NewReceiver(compress bool) *Receiver {
	receiver := &Receiver{}
	receiver.buffer = make([]byte, conf.MaxPacketSize*2)
	if compress {
		receiver.unCompressor = &ZipUnCompressor{}
	}
	return receiver
}

func (this *Receiver) unPack() (ret interface{}, err error) {
	unpackSize := uint64(this.w - this.r)
	if unpackSize >= minSize {

		var payload uint32
		var cmd uint16
		var err error
		var buff []byte
		var msg proto.Message
		var totalSize uint64
		var flag byte

		for {

			reader := kendynet.NewReader(kendynet.NewByteBuffer(this.buffer[this.r:], unpackSize))
			if payload, err = reader.GetUint32(); err != nil {
				break
			}

			if uint64(payload) == 0 {
				err = fmt.Errorf("zero payload")
				break
			}

			if uint64(payload)+sizeLen > conf.MaxPacketSize {
				err = fmt.Errorf("large packet %d", uint64(payload)+sizeLen)
				break
			}

			totalSize = uint64(payload + sizeLen)

			if totalSize <= unpackSize {

				if flag, err = reader.GetByte(); err != nil {
					break
				}

				if cmd, err = reader.GetUint16(); err != nil {
					break
				}
				//普通消息
				size := payload - sizeCmd - sizeFlag
				if buff, err = reader.GetBytes(uint64(size)); err != nil {
					break
				}

				if flag == byte(1) {
					if nil == this.unCompressor {
						err = fmt.Errorf("invaild compress packet")
						break
					}

					if buff, err = this.unCompressor.UnCompress(buff); err != nil {
						break
					}
				}

				if msg, err = pb.Unmarshal(uint32(cmd), buff); err != nil {
					break
				}
				this.r += totalSize
				ret = NewMessage(pb.GetNameByID(uint32(cmd)), msg)
			}
			break
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
			} else if uint64(cap(this.buffer))-this.w < conf.MaxPacketSize/4 {
				copy(this.buffer, this.buffer[this.r:this.w])
				this.w = this.w - this.r
				this.r = 0
			}

			conn := sess.GetUnderConn().(*net.TCPConn)
			n, err := conn.Read(this.buffer[this.w:])

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
