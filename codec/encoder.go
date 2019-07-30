package codec

import (
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/sniperHW/flyfish/codec/pb"
	"github.com/sniperHW/flyfish/conf"
	_ "github.com/sniperHW/flyfish/proto"
	"github.com/sniperHW/kendynet"
)

const (
	sizeLen  = 4
	sizeFlag = 1
	sizeCmd  = 2
)

type Encoder struct {
	compressor CompressorI
}

func NewEncoder(compress bool) *Encoder {
	e := &Encoder{}
	if compress {
		e.compressor = &ZipCompressor{}
	}

	return e
}

type outMessage struct {
	compressor CompressorI
	msg        proto.Message
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
	var flag byte
	if pbbytes, cmd, err = pb.Marshal(this.msg); err != nil {
		kendynet.Errorln("outMessage encode err:", err)
		return nil
	}

	if this.compressor != nil && len(pbbytes) >= 1024 {
		pbbytes, _ = this.compressor.Compress(pbbytes)
		flag = byte(1)
	}

	payloadLen = sizeFlag + sizeCmd + len(pbbytes)
	totalLen = sizeLen + payloadLen
	if uint64(totalLen) > conf.MaxPacketSize {
		kendynet.Errorln("packet too large totalLen", totalLen)
		return nil
	}
	//len + flag + cmd + pbbytes
	buff := kendynet.NewByteBuffer(totalLen)
	//写payload大小
	buff.AppendUint32(uint32(payloadLen))
	//写flag
	buff.AppendByte(flag)
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
			msg:        o.(proto.Message),
			compressor: this.compressor,
		}, nil
		break
	default:
		break
	}
	return nil, fmt.Errorf("invaild object type")
}
