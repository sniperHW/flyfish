// +build !aio

package kvproxy

import (
	"fmt"
	"github.com/sniperHW/flyfish/conf"
	"github.com/sniperHW/flyfish/net"
	"github.com/sniperHW/kendynet"
	"github.com/sniperHW/kendynet/socket"
)

type Receiver struct {
	buffer         []byte
	w              uint64
	r              uint64
	nextPacketSize uint64
}

func NewReceiver() *Receiver {
	receiver := &Receiver{}
	receiver.buffer = make([]byte, initBufferSize)
	return receiver
}

func (this *Receiver) unPack() (ret interface{}, err error) {
	unpackSize := uint64(this.w - this.r)
	if unpackSize > minSize {
		var payload uint32
		var totalSize uint64
		reader := kendynet.NewReader(kendynet.NewByteBuffer(this.buffer[this.r:], unpackSize))
		if payload, err = reader.GetUint32(); err != nil {
			return
		}

		if uint64(payload) == 0 {
			err = fmt.Errorf("zero payload")
			return
		}

		if uint64(payload)+net.SizeLen > conf.MaxPacketSize {
			err = fmt.Errorf("large packet %d", uint64(payload)+net.SizeLen)
			return
		}

		totalSize = uint64(payload + net.SizeLen)

		this.nextPacketSize = totalSize

		if totalSize <= unpackSize {
			msg := kendynet.NewByteBuffer(totalSize)
			err = msg.AppendBytes(this.buffer[this.r : this.r+totalSize])
			if nil == err {
				this.nextPacketSize = 0
				this.r += totalSize
				ret = msg
			}
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
