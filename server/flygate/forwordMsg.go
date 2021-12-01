package flygate

import (
	"container/list"
	"github.com/sniperHW/flyfish/errcode"
	"github.com/sniperHW/flyfish/pkg/buffer"
	flynet "github.com/sniperHW/flyfish/pkg/net"
	"github.com/sniperHW/flyfish/pkg/net/cs"
	"sync/atomic"
	"time"
)

type forwordMsg struct {
	seqno           int64
	leaderVersion   int64
	slot            int
	oriSeqno        int64
	cmd             uint16
	deadline        time.Time
	bytes           []byte
	deadlineTimer   *time.Timer
	cli             *flynet.Socket
	replyed         int32
	listElement     *list.Element
	l               *list.List
	totalPendingMsg *int64
	waitResponse    *map[int64]*forwordMsg
	store           *store
}

func (r *forwordMsg) onTimeout() {
	if nil != r.waitResponse {
		delete(*r.waitResponse, r.seqno)
		r.waitResponse = nil
	}

	if nil != r.l && nil != r.listElement {
		r.l.Remove(r.listElement)
		r.l = nil
		r.listElement = nil
	}

	r.dropReply()
}

func (r *forwordMsg) reply(b []byte) {
	if atomic.CompareAndSwapInt32(&r.replyed, 0, 1) {
		atomic.AddInt64(r.totalPendingMsg, -1)
		r.cli.Send(b)
	}
}

func (r *forwordMsg) replyErr(err errcode.Error) {
	if atomic.CompareAndSwapInt32(&r.replyed, 0, 1) {
		atomic.AddInt64(r.totalPendingMsg, -1)
		replyCliError(r.cli, r.seqno, r.cmd, err)
	}
}

func (r *forwordMsg) dropReply() {
	if atomic.CompareAndSwapInt32(&r.replyed, 0, 1) {
		atomic.AddInt64(r.totalPendingMsg, -1)
	}
}

func replyCliError(cli *flynet.Socket, seqno int64, cmd uint16, err errcode.Error) {

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

	b = buffer.AppendInt32(b, 0)

	cli.Send(b)
}
