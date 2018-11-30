package codec

import (
	"github.com/golang/protobuf/proto"
	"github.com/sniperHW/kendynet"
	"fmt"
	"os"
	"flyfish/codec/pb"
	"flyfish/conf"
	_ "flyfish/proto"
)

const (
	sizeLen      = 4
	sizeCmd      = 2
)

type Encoder struct {
}

func NewEncoder() *Encoder {
	return &Encoder{}
}

func (this *Encoder) EnCode(o interface{}) (kendynet.Message, error) {
	var pbbytes []byte
	var cmd uint32
	var err error
	var payloadLen int
	var totalLen int
	switch o.(type) {
	case proto.Message:
		if pbbytes, cmd, err = pb.Marshal(o); err != nil {
			return nil, err
		}
		payloadLen = sizeCmd + len(pbbytes)
		totalLen = sizeLen + payloadLen
		if uint64(totalLen) > conf.MaxPacketSize {
			return nil, fmt.Errorf("packet too large totalLen:%d", totalLen)
		}
		//len + flag + cmd + pbbytes
		buff := kendynet.NewByteBuffer(totalLen)
		//写payload大小
		buff.AppendUint32(uint32(payloadLen))
		//写cmd
		buff.AppendUint16(uint16(cmd))
		//写数据
		buff.AppendBytes(pbbytes)
		if totalLen != len(buff.Bytes()) {
			kendynet.Errorf("totalLen error %d %d\n", totalLen, len(buff.Bytes()))
			os.Exit(0)
		}
		return buff, nil
		break
	default:
		break
	}
	return nil, fmt.Errorf("invaild object type")
}
