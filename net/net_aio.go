// +build linux darwin netbsd freebsd openbsd dragonfly

package net

import (
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/sniperHW/flyfish/conf"
	"github.com/sniperHW/flyfish/net/pb"
	"github.com/sniperHW/kendynet"
	"github.com/sniperHW/kendynet/socket/aio"
	"net"
	"runtime"
)

var aioService *aio.AioService

func init() {
	aioService = aio.NewAioService(1, runtime.NumCPU()*2, runtime.NumCPU()*2, nil)
}

func createSession(conn net.Conn) kendynet.StreamSession {
	return aio.NewAioSocket(aioService, conn)
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
			return nil, sess.(*aio.AioSocket).Recv(this.buffer[this.w:])
		} else {
			return nil, err
		}
	}
}

/*
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
	if compress {
		receiver.unCompressor = &ZipUnCompressor{}
	}
	return receiver
}

func (this *Receiver) OnRecvOk(s kendynet.StreamSession, buff []byte) {
	if cap(this.buffer) == 0 {
		this.buffer = buff
		this.r = 0
		this.w = uint64(len(buff))
	} else {
		l := uint64(len(buff))
		space := uint64(len(this.buffer)) - (this.w - this.r)
		if space < l {
			buffer := make([]byte, (this.w-this.r)+l)
			copy(buffer, this.buffer[this.r:this.w])
			copy(buffer[this.w-this.r:], buff[:l])
			buffPool.Put(this.buffer)
			this.buffer = buffer
			this.w = (this.w - this.r) + l
			this.r = 0
		} else {
			if uint64(len(this.buffer))-this.w >= l {
				copy(this.buffer[this.w:], buff[:l])
				this.w += l
			} else {
				copy(this.buffer, this.buffer[this.r:this.w])
				this.w -= this.r
				copy(this.buffer[this.w:], buff[:l])
				this.w += l
				this.r = 0
			}
			buffPool.Put(buff)
		}
	}
}

func (this *Receiver) StartReceive(s kendynet.StreamSession) {
	s.(*aio.AioSocket).Recv(nil)
}

func (this *Receiver) OnClose() {
	if nil != this.buffer {
		buffPool.Put(this.buffer)
		this.buffer = nil
	}
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

func (this *Receiver) ReceiveAndUnpack(s kendynet.StreamSession) (interface{}, error) {
	msg, err := this.unPack()
	if nil == msg && nil == err {
		return nil, s.(*aio.AioSocket).Recv(this.buffer)
	}

	if nil != msg && this.r == this.w {
		buffPool.Put(this.buffer)
		this.buffer = nil
	}

	return msg, err
}
*/
