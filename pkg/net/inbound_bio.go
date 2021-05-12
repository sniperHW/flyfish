// +build !aio

package net

import (
	"github.com/sniperHW/flyfish/pkg/net/pb"
)

func (this *InboundProcessor) GetRecvBuff() []byte {
	return this.buffer[this.w:]
}

func (this *InboundProcessor) OnData(data []byte) {
	this.w += len(data)
}

func (this *InboundProcessor) Unpack() (interface{}, error) {
	if this.r == this.w {
		return nil, nil
	} else {
		msg, packetSize, err := this.unpack(this.pbSpace, this.buffer, this.r, this.w)
		if nil != msg {
			this.r += packetSize
			if this.r == this.w {
				this.r = 0
				this.w = 0
			}
		} else if nil == err {
			if packetSize > cap(this.buffer) {
				buffer := make([]byte, sizeofPow2(packetSize))
				copy(buffer, this.buffer[this.r:this.w])
				this.buffer = buffer
			} else {
				//空间足够容纳下一个包，
				copy(this.buffer, this.buffer[this.r:this.w])
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

	return receiver
}
