package codec

import (
	//"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/sniperHW/flyfish/codec/pb"
	"github.com/sniperHW/flyfish/conf"
	_ "github.com/sniperHW/flyfish/proto"
	"github.com/sniperHW/kendynet"
)

const (
	SizeLen  = 4
	SizeFlag = 1
	SizeCmd  = 2
)

type Encoder struct {
	compressor CompressorI
	pbSpace    *pb.Namespace
}

func NewEncoder(pbSpace *pb.Namespace, compress bool) *Encoder {
	e := &Encoder{
		pbSpace: pbSpace,
	}
	if compress {
		e.compressor = &ZipCompressor{}
	}

	return e
}

type outMessage struct {
	compressor CompressorI
	head       CommonHead
	msg        proto.Message
	pbSpace    *pb.Namespace
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
	if pbbytes, cmd, err = this.pbSpace.Marshal(this.msg); err != nil {
		kendynet.Errorln("outMessage encode err:", err, this.pbSpace.Name())
		return nil
	}

	if this.compressor != nil && len(pbbytes) >= 1024 {
		pbbytes, _ = this.compressor.Compress(pbbytes)
		flag = byte(1)
	}

	sizeOfUniKey := len(this.head.UniKey)

	sizeOfHead := 8 + 4 + 4 + 2 + sizeOfUniKey //int64 + int32 + uint32 + int16

	payloadLen = SizeFlag + SizeCmd + len(pbbytes) + sizeOfHead
	totalLen = SizeLen + payloadLen
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
	//写head
	buff.AppendInt64(this.head.Seqno)
	buff.AppendInt32(this.head.ErrCode)
	buff.AppendUint32(this.head.Timeout)
	buff.AppendInt16(int16(sizeOfUniKey))
	if sizeOfUniKey > 0 {
		buff.AppendString(this.head.UniKey)
	}
	//写cmd
	buff.AppendUint16(uint16(cmd))
	//写数据
	buff.AppendBytes(pbbytes)
	return buff.Bytes()
}

func (this *Encoder) EnCode(o interface{}) (kendynet.Message, error) {
	kendynet.Infoln("Encoder")
	msg := o.(*Message)
	return &outMessage{
		msg:        msg.GetData(),
		head:       msg.GetHead(),
		compressor: this.compressor,
		pbSpace:    this.pbSpace,
	}, nil
}
