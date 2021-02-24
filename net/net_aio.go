// +build aio

package net

import (
	"fmt"
	"github.com/gogo/protobuf/proto"
	"github.com/sniperHW/flyfish/conf"
	"github.com/sniperHW/flyfish/net/pb"
	"github.com/sniperHW/kendynet"
	"github.com/sniperHW/kendynet/socket/aio"
	"net"
	"sync"
)

type BufferPool struct {
	pool sync.Pool
}

const PoolBuffSize uint64 = 1024 * 1024 * 2

func NewBufferPool() *BufferPool {
	return &BufferPool{
		pool: sync.Pool{
			New: func() interface{} {
				return make([]byte, PoolBuffSize)
			},
		},
	}
}

func (p *BufferPool) Acquire() []byte {
	return p.pool.Get().([]byte)
}

func (p *BufferPool) Release(buff []byte) {
	if uint64(cap(buff)) == PoolBuffSize {
		p.pool.Put(buff[:cap(buff)])
	}
}

var aioService *aio.SocketService
var buffPool *BufferPool

func init() {
	buffPool = NewBufferPool()
	aioService = aio.NewSocketService(buffPool)
}

func createSession(conn net.Conn) kendynet.StreamSession {
	return aio.NewSocket(aioService, conn)
}

type AioReceiverBase struct {
	buffer     []byte
	w          uint64
	r          uint64
	BaseUnpack func(buffer []byte, w uint64, r uint64) (ret interface{}, packetSize uint64, err error)
}

type Receiver struct {
	*AioReceiverBase
	unCompressor UnCompressorI
	pbSpace      *pb.Namespace
}

//供aio socket使用
func (this *AioReceiverBase) GetRecvBuff() []byte {
	if len(this.buffer) == 0 {
		//sharebuffer模式
		return nil
	} else {
		//之前有包没接收完，先把这个包接收掉
		return this.buffer[this.w:]
	}
}

func (this *AioReceiverBase) OnData(data []byte) {
	if len(this.buffer) == 0 {
		this.buffer = data
	}
	this.w += uint64(len(data))
}

func (this *AioReceiverBase) Unpack() (interface{}, error) {
	if this.r == this.w {
		return nil, nil
	} else {
		msg, packetSize, err := this.BaseUnpack(this.buffer, this.r, this.w)
		if nil != msg {
			this.r += packetSize
			if this.r == this.w {
				this.r = 0
				this.w = 0
				buffPool.Release(this.buffer)
				this.buffer = nil
			}
		} else if nil == err {
			//新开一个buffer把未接完整的包先接收掉,处理完这个包之后再次启用sharebuffer模式
			buff := make([]byte, packetSize)
			copy(buff, this.buffer[this.r:this.w])
			this.w = this.w - this.r
			this.r = 0
			buffPool.Release(this.buffer)
			this.buffer = buff
		}
		return msg, err
	}
}

func (this *AioReceiverBase) OnSocketClose() {
	buffPool.Release(this.buffer)
}

func (this *AioReceiverBase) ReceiveAndUnpack(s kendynet.StreamSession) (interface{}, error) {
	return nil, nil
}

func NewReceiver(pbSpace *pb.Namespace, compress bool) *Receiver {
	receiver := &Receiver{
		pbSpace: pbSpace,
	}

	receiver.AioReceiverBase = &AioReceiverBase{
		BaseUnpack: receiver.unpack,
	}

	if compress {
		receiver.unCompressor = &ZipUnCompressor{}
	}
	return receiver
}

func (this *Receiver) unpack(buffer []byte, r uint64, w uint64) (ret interface{}, packetSize uint64, err error) {

	unpackSize := uint64(w - r)
	if unpackSize > minSize {
		var payload uint32
		var cmd uint16
		var buff []byte
		var msg proto.Message
		var totalSize uint64
		var flag byte

		reader := kendynet.NewReader(kendynet.NewByteBuffer(buffer[r:], unpackSize))
		if payload, err = reader.GetUint32(); err != nil {
			fmt.Println(err)
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

		packetSize = totalSize

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
			ret = NewMessageWithCmd(uint16(cmd), head, msg)
		}
	}
	return
}
