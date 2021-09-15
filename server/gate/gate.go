package gate

import (
	"encoding/binary"
	"fmt"
	"github.com/sniperHW/flyfish/errcode"
	"github.com/sniperHW/flyfish/pkg/buffer"
	"github.com/sniperHW/flyfish/pkg/net/cs"
	"github.com/sniperHW/flyfish/pkg/net/pb"
	"github.com/sniperHW/flyfish/server/slot"
	"github.com/sniperHW/kendynet"
	"net"
	"sync"
	"unsafe"
)

type node struct {
	service        string
	consoleAddr    *net.UDPAddr
	serviceSession kendynet.StreamSession
}

type relayMsg struct {
	slot  int
	seqno int64
	cmd   uint16
	bytes []byte
}

func replyCliError(cli kendynet.StreamSession, seqno int64, cmd uint16, err errcode.Error) {

	var sizeOfErrDesc int

	if nil != err && err.Code != 0 {
		sizeOfErrDesc = len(err.Desc)
		if sizeOfErrDesc > 0xFF {
			//描述超长，直接丢弃
			sizeOfErrDesc = cs.SizeErrDescLen
		} else {
			sizeOfErrDesc += cs.SizeErrDescLen
		}
	}

	payloadLen := cs.SizeSeqNo + cs.SizeCmd + cs.SizeErrCode + sizeOfErrDesc + cs.SizePB
	totalLen := cs.SizeLen + payloadLen
	if uint64(totalLen) > cs.MaxPacketSize {
		return
	}

	b := make([]byte, 0, totalLen)

	//写payload大小
	b = buffer.AppendUint32(b, uint32(payloadLen))
	//seqno
	b = buffer.AppendInt64(b, seqno)
	//cmd
	b = buffer.AppendUint16(b, cmd)
	//err
	b = buffer.AppendInt16(b, errcode.GetCode(err))

	if sizeOfErrDesc > 0 {
		b = buffer.AppendUint16(b, uint16(sizeOfErrDesc-cs.SizeErrDescLen))
		if sizeOfErrDesc > cs.SizeErrDescLen {
			b = buffer.AppendString(b, err.Desc)
		}
	}

	cli.Send(b)
}

func clientReqUnpack(pbSpace *pb.Namespace, b []byte, r int, w int) (ret interface{}, packetSize int, err error) {
	unpackSize := w - r
	if unpackSize >= cs.MinSize {
		payload := int(binary.BigEndian.Uint32(b[r : r+cs.SizeLen]))

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
			m.bytes = b[r : r+totalSize]
			m.seqno = int64(binary.BigEndian.Uint64(b[rr : rr+8]))
			rr += (8 + 4)
			m.cmd = binary.BigEndian.Uint16(b[rr : rr+2])
			rr += 2
			uniKeyLen := int(binary.BigEndian.Uint16(b[rr : rr+2]))
			if uniKeyLen > 0 {
				rr += 2
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

type store struct {
	sync.Mutex
	queryingLeader bool
	dailing        bool
	leader         *node
	nodes          []*node
	waittingSend   []*relayMsg
}

func (s *store) onCliMsg(cli kendynet.StreamSession, msg *relayMsg) {

}

type gate struct {
	slotToStore map[int]*store
}

func (g *gate) onCliMsg(cli kendynet.StreamSession, msg *relayMsg) {
	s, ok := g.slotToStore[msg.slot]
	if !ok {
		replyCliError(cli, msg.seqno, msg.cmd, errcode.New(errcode.Errcode_error, "can't find store"))
	} else {
		s.onCliMsg(cli, msg)
	}
}
