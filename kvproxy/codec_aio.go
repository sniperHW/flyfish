// +build linux darwin netbsd freebsd openbsd dragonfly

package kvproxy

import (
	"fmt"
	"github.com/sniperHW/flyfish/conf"
	"github.com/sniperHW/flyfish/net"
	"github.com/sniperHW/kendynet"
)

type Receiver struct {
	*net.AioReceiverBase
}

func NewReceiver() *Receiver {
	receiver := &Receiver{}
	receiver.AioReceiverBase = &net.AioReceiverBase{
		Unpack: receiver.unpack,
	}
	return receiver
}

func (this *Receiver) unpack(buffer []byte, r uint64, w uint64) (ret interface{}, packetSize uint64, nextPacketSize uint64, err error) {
	unpackSize := uint64(w - r)
	if unpackSize > minSize {
		var payload uint32
		var totalSize uint64
		reader := kendynet.NewReader(kendynet.NewByteBuffer(buffer[r:], unpackSize))
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

		nextPacketSize = totalSize

		if totalSize <= unpackSize {
			msg := kendynet.NewByteBuffer(totalSize)
			err = msg.AppendBytes(buffer[r : r+totalSize])
			if nil == err {
				nextPacketSize = 0
				packetSize = totalSize
				ret = msg
			}
		}
	}
	return
}

/*
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

func (this *Receiver) StartReceive(s kendynet.StreamSession) {
	s.(*aio.AioSocket).Recv(this.buffer)
}

func (this *Receiver) OnClose() {

}

func (this *Receiver) OnRecvOk(s kendynet.StreamSession, buff []byte) {
	this.w += uint64(len(buff))
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

			return nil, sess.(*aio.AioSocket).Recv(this.buffer[this.w:])

		} else {
			return nil, err
		}
	}
}
*/
