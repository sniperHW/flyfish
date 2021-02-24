// +build aio

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
		BaseUnpack: receiver.unpack,
	}
	return receiver
}

func (this *Receiver) unpack(buffer []byte, r uint64, w uint64) (ret interface{}, packetSize uint64, err error) {
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
		packetSize = totalSize

		if totalSize <= unpackSize {
			msg := kendynet.NewByteBuffer(totalSize)
			err = msg.AppendBytes(buffer[r : r+totalSize])
			if nil == err {
				ret = msg
			}
		}
	}
	return
}
