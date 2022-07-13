package flygate

import (
	"encoding/binary"
	"github.com/sniperHW/flyfish/errcode"
	"github.com/sniperHW/flyfish/pkg/buffer"
	flynet "github.com/sniperHW/flyfish/pkg/net"
	"github.com/sniperHW/flyfish/proto/cs"
	"sync/atomic"
	"time"
)

type containerElement interface {
	remove()
	container() container
}

type container interface {
	add(*request)
	remove(containerElement)
}

type request struct {
	seqno             int64
	slot              int
	bytes             []byte
	deadline          time.Time
	deadlineTimer     *time.Timer
	replied           int32
	from              *flynet.Socket
	to                *flynet.Socket
	containerElements []containerElement
	leaderVersion     int64
	store             uint64 //high32:setid,low32:storeid
}

func (r *request) removeAll() {
	for len(r.containerElements) > 0 {
		r.containerElements[0].remove()
	}
}

func (r *request) remove(c container) {
	for _, v := range r.containerElements {
		if v.container() == c {
			v.remove()
			return
		}
	}
}

func makeErrResponse(seqno int64, cmd uint16, err errcode.Error) []byte {
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

	payloadLen := cs.SizeSeqNo + cs.SizeCmd + cs.SizeErrCode + sizeOfErrDesc + cs.SizeCompress

	b := make([]byte, 0, cs.SizeLen+payloadLen)

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

	b = buffer.AppendByte(b, byte(0))

	return b
}

func (r *request) cmd() uint16 {
	return binary.BigEndian.Uint16(r.bytes[cs.SizeLen+12:])
}

func (r *request) reply(b []byte) {
	if atomic.CompareAndSwapInt32(&r.replied, 0, 1) {
		r.deadlineTimer.Stop()
		r.removeAll()
		r.from.Send(b)
	}
}

func (r *request) replyErr(err errcode.Error) {
	r.reply(makeErrResponse(r.seqno, r.cmd(), err))
}

func (r *request) dropReply() {
	if atomic.CompareAndSwapInt32(&r.replied, 0, 1) {
		r.deadlineTimer.Stop()
		r.removeAll()
	}
}
