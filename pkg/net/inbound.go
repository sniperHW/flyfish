package net

import (
	"github.com/sniperHW/flyfish/pkg/net/pb"
	"sync"
)

var DefaultRecvBuffSize int = 64 * 1024

const (
	m512K = 512 * 1024
	m1M   = 1024 * 1024
	m2M   = 2 * 1024 * 1024
	m4M   = 4 * 1024 * 1024
	m8M   = 8 * 1024 * 1024
)

func getPoolIndex(cap int) int {
	if cap <= m512K {
		return 0
	} else if cap <= m1M {
		return 1
	} else if cap <= m2M {
		return 2
	} else if cap <= m4M {
		return 3
	} else if cap <= m8M {
		return 4
	} else {
		return -1
	}
}

type inboundBufferPool struct {
	fixedBufferPool []*sync.Pool
}

func (this *inboundBufferPool) get(cap int) []byte {
	i := getPoolIndex(cap)
	if i < 0 {
		return make([]byte, cap)
	} else {
		return this.fixedBufferPool[i].Get().([]byte)
	}
}

func (this *inboundBufferPool) put(b []byte) {
	if i := getPoolIndex(cap(b)); i >= 0 {
		this.fixedBufferPool[i].Put(b)
	}
}

var gInboundBufferPool inboundBufferPool = inboundBufferPool{
	fixedBufferPool: []*sync.Pool{
		&sync.Pool{
			New: func() interface{} {
				return make([]byte, m512K)
			},
		},
		&sync.Pool{
			New: func() interface{} {
				return make([]byte, m1M)
			},
		},
		&sync.Pool{
			New: func() interface{} {
				return make([]byte, m2M)
			},
		},
		&sync.Pool{
			New: func() interface{} {
				return make([]byte, m4M)
			},
		},
		&sync.Pool{
			New: func() interface{} {
				return make([]byte, m8M)
			},
		},
	},
}

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
					gInboundBufferPool.put(*this.pBuffer)
					//大包接收完毕，释放extBuffer
					this.pBuffer = &this.buffer
				}
			}
		} else if nil == err {
			if packetSize > cap(*this.pBuffer) {
				buffer := gInboundBufferPool.get(packetSize)
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

func NewInboundProcessor(unpack func(*pb.Namespace, []byte, int, int) (interface{}, int, error), pbSpace *pb.Namespace) *InboundProcessor {

	receiver := &InboundProcessor{
		pbSpace: pbSpace,
		unpack:  unpack,
		buffer:  make([]byte, DefaultRecvBuffSize),
	}
	receiver.pBuffer = &receiver.buffer

	return receiver
}
