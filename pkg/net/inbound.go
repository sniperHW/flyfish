// +build !aio

package net

import (
	"github.com/sniperHW/flyfish/pkg/net/pb"
)

type InboundProcessor struct {
	buffer  []byte
	pBuffer *[]byte
	w       int
	r       int
	pbSpace *pb.Namespace
	unpack  func(*pb.Namespace, []byte, int, int) (interface{}, int, error)
}

func (this *InboundProcessor) GetRecvBuff() []byte {
	return (*this.pBuffer)[this.w:]
}

func (this *InboundProcessor) OnData(data []byte) {
	this.w += len(data)
}

func (this *InboundProcessor) Unpack() (interface{}, error) {
	if this.r == this.w {
		return nil, nil
	} else {
		msg, packetSize, err := this.unpack(this.pbSpace, *this.pBuffer, this.r, this.w)
		if nil != msg {
			this.r += packetSize
			if this.r == this.w {
				this.r = 0
				this.w = 0
				if this.pBuffer != &this.buffer {
					//大包接收完毕，释放extBuffer
					this.pBuffer = &this.buffer
				}
			}
		} else if nil == err {
			if packetSize > cap(*this.pBuffer) {
				buffer := make([]byte, packetSize)
				copy(buffer, (*this.pBuffer)[this.r:this.w])
				this.pBuffer = &buffer
			} else {
				//空间足够容纳下一个包，
				copy(*this.pBuffer, (*this.pBuffer)[this.r:this.w])
			}
			this.w = this.w - this.r
			this.r = 0
		}
		return msg, err
	}
}

func (this *InboundProcessor) OnSocketClose() {

}

func NewInboundProcessor(unpack func(*pb.Namespace, []byte, int, int) (interface{}, int, error), pbSpace *pb.Namespace) *InboundProcessor {

	receiver := &InboundProcessor{
		pbSpace: pbSpace,
		unpack:  unpack,
		buffer:  make([]byte, 64*1024),
	}
	receiver.pBuffer = &receiver.buffer

	return receiver
}
