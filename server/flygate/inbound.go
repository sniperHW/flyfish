package flygate

import (
	"encoding/binary"
	"fmt"
	flynet "github.com/sniperHW/flyfish/pkg/net"
	"github.com/sniperHW/flyfish/pkg/net/cs"
	"github.com/sniperHW/flyfish/pkg/net/pb"
	"github.com/sniperHW/flyfish/server/slot"
	"time"
	"unsafe"
)

func clientReqUnpack(pbSpace *pb.Namespace, b []byte, r int, w int) (ret interface{}, packetSize int, err error) {
	unpackSize := w - r
	if unpackSize >= cs.MinSize {
		payload := int(binary.BigEndian.Uint32(b[r:]))

		if payload == 0 {
			err = fmt.Errorf("zero payload")
			return
		}

		if payload+cs.SizeLen > cs.MaxPacketSize {
			err = fmt.Errorf("large packet %d", payload+cs.SizeLen)
			return
		}

		totalSize := payload + cs.SizeLen

		packetSize = totalSize

		if totalSize <= unpackSize {
			rr := r + cs.SizeLen
			var m relayMsg
			m.bytes = make([]byte, totalSize)
			copy(m.bytes, b[r:r+totalSize])
			m.oriSeqno = int64(binary.BigEndian.Uint64(b[rr:]))
			rr += (8 + 4)
			m.cmd = binary.BigEndian.Uint16(b[rr:])
			rr += 2
			m.deadline = time.Now().Add(time.Duration(binary.BigEndian.Uint32(b[rr:])) * time.Millisecond)
			rr += 4
			uniKeyLen := int(binary.BigEndian.Uint16(b[rr:]))
			rr += 2
			if uniKeyLen > 0 {
				if uniKeyLen > payload-(rr-r) {
					err = fmt.Errorf("invaild uniKeyLen")
					return
				}
				bb := b[rr : rr+uniKeyLen]
				uniKey := *(*string)(unsafe.Pointer(&bb))
				m.slot = slot.Unikey2Slot(uniKey)
			}
			ret = &m
		}
	}
	return
}

func kvnodeRespUnpack(pbSpace *pb.Namespace, b []byte, r int, w int) (ret interface{}, packetSize int, err error) {
	unpackSize := w - r
	if unpackSize >= cs.MinSize {
		payload := int(binary.BigEndian.Uint32(b[r:]))

		if payload == 0 {
			err = fmt.Errorf("zero payload")
			return
		}

		if payload+cs.SizeLen > cs.MaxPacketSize {
			err = fmt.Errorf("large packet %d", payload+cs.SizeLen)
			return
		}

		totalSize := payload + cs.SizeLen

		packetSize = totalSize

		if totalSize <= unpackSize {
			data := make([]byte, totalSize)
			copy(data, b[r:r+totalSize])
			ret = data
		}
	}
	return
}

func NewCliReqInboundProcessor() *flynet.InboundProcessor {
	return flynet.NewInboundProcessor(clientReqUnpack, pb.GetNamespace("request"))
}

func NewKvnodeRespInboundProcessor() *flynet.InboundProcessor {
	return flynet.NewInboundProcessor(kvnodeRespUnpack, pb.GetNamespace("response"))
}
