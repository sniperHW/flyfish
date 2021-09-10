package cs

import (
	"fmt"
	"github.com/gogo/protobuf/proto"
	"github.com/sniperHW/flyfish/errcode"
	"github.com/sniperHW/flyfish/pkg/buffer"
	flynet "github.com/sniperHW/flyfish/pkg/net"
	"github.com/sniperHW/flyfish/pkg/net/pb"
	flyproto "github.com/sniperHW/flyfish/proto"
)

func reqUnpack(pbSpace *pb.Namespace, b []byte, r int, w int) (ret interface{}, packetSize int, err error) {
	unpackSize := w - r
	if unpackSize >= minSize {
		var msg proto.Message

		reader := buffer.NewReader(b[r : r+unpackSize])
		payload := int(reader.GetUint32())

		if payload == 0 {
			err = fmt.Errorf("zero payload")
			return
		}

		if payload+SizeLen > MaxPacketSize {
			err = fmt.Errorf("large packet %d", payload+SizeLen)
			return
		}

		totalSize := payload + SizeLen

		packetSize = totalSize

		if totalSize <= unpackSize {
			m := &ReqMessage{}
			m.Seqno = reader.GetInt64()
			m.Store = int(reader.GetUint32())
			m.Cmd = flyproto.CmdType(reader.GetUint16())
			sizeOfUniKey := int(reader.GetUint16())
			m.UniKey = reader.GetString(sizeOfUniKey)
			m.Timeout = reader.GetUint32()
			pbsize := int(reader.GetInt32())
			buff := reader.GetBytes(pbsize)
			if msg, err = pbSpace.Unmarshal(uint32(m.Cmd), buff); err != nil {
				return
			} else {
				m.Data = msg
				ret = m
			}
		}
	}
	return
}

func respUnpack(pbSpace *pb.Namespace, b []byte, r int, w int) (ret interface{}, packetSize int, err error) {
	unpackSize := w - r
	if unpackSize >= minSize {
		var msg proto.Message

		reader := buffer.NewReader(b[r : r+unpackSize])
		payload := int(reader.GetUint32())

		if payload == 0 {
			err = fmt.Errorf("zero payload")
			return
		}

		if payload+SizeLen > MaxPacketSize {
			err = fmt.Errorf("large packet %d", payload+SizeLen)
			return
		}

		totalSize := payload + SizeLen

		packetSize = totalSize

		if totalSize <= unpackSize {
			m := &RespMessage{}
			m.Seqno = reader.GetInt64()
			m.Cmd = flyproto.CmdType(reader.GetUint16())
			errCode := reader.GetInt16()

			if errCode == 0 {
				m.Err = nil
			} else {
				errDesc := ""
				sizeOfErrDesc := int(reader.GetUint16())
				if sizeOfErrDesc > 0 {
					errDesc = reader.GetString(sizeOfErrDesc)
				}
				m.Err = errcode.New(errCode, errDesc)
			}

			pbsize := int(reader.GetInt32())
			buff := reader.GetBytes(pbsize)

			if msg, err = pbSpace.Unmarshal(uint32(m.Cmd), buff); err != nil {
				return
			} else {
				m.Data = msg
				ret = m
			}
		}
	}
	return
}

func NewReqInboundProcessor() *flynet.InboundProcessor {
	return flynet.NewInboundProcessor(reqUnpack, pb.GetNamespace("request"))
}

func NewRespInboundProcessor() *flynet.InboundProcessor {
	return flynet.NewInboundProcessor(respUnpack, pb.GetNamespace("response"))
}
