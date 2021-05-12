// +build aio

package net

import (
	"github.com/sniperHW/flyfish/pkg/net/pb"
)

//供aio socket使用
func (this *InboundProcessor) GetRecvBuff() []byte {
	if len(this.buffer) == 0 {
		//sharebuffer模式
		return nil
	} else {
		//之前有包没接收完，先把这个包接收掉
		return this.buffer[this.w:]
	}
}

func (this *InboundProcessor) OnData(data []byte) {
	if len(this.buffer) == 0 {
		this.buffer = data
	}
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
				GetBuffPool().Release(this.buffer)
				this.buffer = nil
			}
		} else if nil == err {
			//新开一个buffer把未接完整的包先接收掉,处理完这个包之后再次启用sharebuffer模式
			buff := make([]byte, packetSize)
			copy(buff, this.buffer[this.r:this.w])
			this.w = this.w - this.r
			this.r = 0
			GetBuffPool().Release(this.buffer)
			this.buffer = buff
		}
		return msg, err
	}
}

func (this *InboundProcessor) OnSocketClose() {
	GetBuffPool().Release(this.buffer)
}

func NewInboundProcessor(unpack func(*pb.Namespace, []byte, int, int) (interface{}, int, error), pbSpace *pb.Namespace) *InboundProcessor {
	receiver := &InboundProcessor{
		pbSpace: pbSpace,
		unpack:  unpack,
	}
	return receiver
}
