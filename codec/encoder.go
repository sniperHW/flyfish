package codec

import (
	"flyfish/codec/pb"
	"flyfish/conf"
	_ "flyfish/proto"
	"fmt"

	"github.com/golang/protobuf/proto"
	"github.com/sniperHW/kendynet"
)

const (
	sizeLen = 4
	sizeCmd = 2
)

type Encoder struct {
}

func NewEncoder() *Encoder {
	return &Encoder{}
}

type outMessage struct {
	msg proto.Message
}

/*
*  Bytes()在连接的发送goroutine中被调用,以避免Marshal过度占用逻辑goroutine的计算能力
 */

func (this *outMessage) Bytes() []byte {
	var pbbytes []byte
	var cmd uint32
	var err error
	var payloadLen int
	var totalLen int
	if pbbytes, cmd, err = pb.Marshal(this.msg); err != nil {
		kendynet.Errorln("outMessage encode err:", err)
		return nil
	}
	payloadLen = sizeCmd + len(pbbytes)
	totalLen = sizeLen + payloadLen
	if uint64(totalLen) > conf.MaxPacketSize {
		kendynet.Errorln("packet too large totalLen", totalLen)
		return nil
	}
	//len + flag + cmd + pbbytes
	buff := kendynet.NewByteBuffer(totalLen)
	//写payload大小
	buff.AppendUint32(uint32(payloadLen))
	//写cmd
	buff.AppendUint16(uint16(cmd))
	//写数据
	buff.AppendBytes(pbbytes)
	return buff.Bytes()
}

func (this *Encoder) EnCode(o interface{}) (kendynet.Message, error) {
	switch o.(type) {
	case proto.Message:
		return &outMessage{
			msg: o.(proto.Message),
		}, nil
		break
	default:
		break
	}
	return nil, fmt.Errorf("invaild object type")
}
