package cs

import (
	"errors"
	"fmt"
	"github.com/sniperHW/flyfish/core/buffer"
	"github.com/sniperHW/flyfish/net/pb"
	_ "github.com/sniperHW/flyfish/proto"
	"reflect"
)

const (
	SizeSeqNo      = 8
	SizeStore      = 4
	SizeLen        = 4
	SizeCmd        = 2
	SizePB         = 4
	SizeErrCode    = 2
	SizeTimeout    = 4
	SizeErrDescLen = 2
	SizeUniKeyLen  = 2
	minSize        = SizeLen
	MaxPacketSize  = 8 * 1024 * 1024
)

type ReqEncoder struct {
	pbSpace *pb.Namespace
}

func (this *ReqEncoder) EnCode(o interface{}, buff *buffer.Buffer) error {

	m, ok := o.(*ReqMessage)

	if !ok {
		if nil == m {
			panic("o is nil")
		}
		return fmt.Errorf("invaild object to encode:%s", reflect.TypeOf(o).String())
	}

	if nil == m.Data {
		return errors.New("Data is nil")
	}

	sizeOfUniKey := len(m.UniKey)

	if sizeOfUniKey > 0xFF {
		return fmt.Errorf("UniKey too large %d", sizeOfUniKey)
	}

	var pbbytes []byte
	var cmd uint32 = uint32(m.Cmd)
	var err error

	if nil == this.pbSpace {
		this.pbSpace = pb.GetNamespace("request")
	}

	if pbbytes, cmd, err = this.pbSpace.Marshal(m.Data); err != nil {
		return nil
	}

	payloadLen := SizeSeqNo + SizeStore + SizeUniKeyLen + sizeOfUniKey + SizeTimeout + SizeCmd + SizePB + len(pbbytes)
	totalLen := SizeLen + payloadLen
	if uint64(totalLen) > MaxPacketSize {
		return nil
	}

	//写payload大小
	buff.AppendUint32(uint32(payloadLen))
	//seqno
	buff.AppendInt64(m.Seqno)
	//store 占位
	buff.AppendUint32(uint32(m.Store))
	//cmd
	buff.AppendUint16(uint16(cmd))
	//unikey
	buff.AppendInt16(int16(sizeOfUniKey))
	buff.AppendString(m.UniKey)
	//timeout
	buff.AppendUint32(m.Timeout)
	//pb
	buff.AppendInt32(int32(len(pbbytes)))
	buff.AppendBytes(pbbytes)
	return nil
}

type RespEncoder struct {
	pbSpace *pb.Namespace
}

func (this *RespEncoder) EnCode(o interface{}, buff *buffer.Buffer) error {

	m, ok := o.(*RespMessage)

	if !ok {
		if nil == m {
			panic("o is nil")
		}
		return fmt.Errorf("invaild object to encode:%s", reflect.TypeOf(o).String())
	}

	if nil == this.pbSpace {
		this.pbSpace = pb.GetNamespace("response")
	}

	var pbbytes []byte
	var cmd uint32 = uint32(m.Cmd)
	var err error
	var sizeOfErrDesc int

	if nil != m.Data {
		if pbbytes, cmd, err = this.pbSpace.Marshal(m.Data); err != nil {
			return nil
		}
	}

	errCode := int16(0)

	if nil != m.Err && m.Err.Code != 0 {
		errCode = m.Err.Code
		sizeOfErrDesc = len(m.Err.Desc)
		if sizeOfErrDesc > 0xFF {
			//描述超长，直接丢弃
			sizeOfErrDesc = SizeErrDescLen
		} else {
			sizeOfErrDesc += SizeErrDescLen
		}
	}

	payloadLen := SizeSeqNo + SizeCmd + SizeErrCode + sizeOfErrDesc + SizePB + len(pbbytes)
	totalLen := SizeLen + payloadLen
	if uint64(totalLen) > MaxPacketSize {
		return nil
	}

	//写payload大小
	buff.AppendUint32(uint32(payloadLen))
	//seqno
	buff.AppendInt64(m.Seqno)
	//cmd
	buff.AppendUint16(uint16(cmd))
	//err
	buff.AppendInt16(errCode)
	if sizeOfErrDesc > 0 {
		buff.AppendUint16(uint16(sizeOfErrDesc - SizeErrDescLen))
		if sizeOfErrDesc > SizeErrDescLen {
			buff.AppendString(m.Err.Desc)
		}
	}
	buff.AppendInt32(int32(len(pbbytes)))
	if len(pbbytes) > 0 {
		//写数据
		buff.AppendBytes(pbbytes)
	}
	return nil
}
