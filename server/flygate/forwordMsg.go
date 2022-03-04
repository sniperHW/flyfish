package flygate

import (
	"container/list"
	"github.com/sniperHW/flyfish/errcode"
	"github.com/sniperHW/flyfish/pkg/buffer"
	flynet "github.com/sniperHW/flyfish/pkg/net"
	"github.com/sniperHW/flyfish/proto/cs"
	"sync/atomic"
	"time"
)

type pending struct {
	msgMap      *map[int64]*forwordMsg
	listElement *list.Element
	l           *list.List
}

type forwordMsg struct {
	pending
	seqno           int64
	leaderVersion   int64
	slot            int
	oriSeqno        int64
	cmd             uint16
	bytes           []byte
	deadline        time.Time
	deadlineTimer   *time.Timer
	cli             *flynet.Socket
	replied         bool
	totalPendingMsg *int64
	store           *store
}

func (r *forwordMsg) remove() {
	r.removeList()
	r.removeMap()
}

func (r *forwordMsg) removeList() {
	if nil != r.l && nil != r.listElement {
		r.l.Remove(r.listElement)
		r.l = nil
		r.listElement = nil
	}
}

func (r *forwordMsg) removeMap() {
	if nil != r.msgMap {
		delete(*r.msgMap, r.seqno)
		r.msgMap = nil
	}
}

func (r *forwordMsg) add(msgMap *map[int64]*forwordMsg, l *list.List) {
	if nil != msgMap {
		r.removeMap()
		(*msgMap)[r.seqno] = r
	}

	if nil != l {
		r.removeList()
		r.l = l
		r.listElement = l.PushBack(r)
	}

}

func (r *forwordMsg) setReplied() bool {
	if !r.replied {
		return false
	} else {
		r.replied = true
		return true
	}
}

func (r *forwordMsg) reply(b []byte) {
	if r.setReplied() {
		atomic.AddInt64(r.totalPendingMsg, -1)
		r.deadlineTimer.Stop()
		r.remove()
		r.cli.Send(b)
	}
}

func (r *forwordMsg) replyErr(err errcode.Error) {
	if r.setReplied() {
		atomic.AddInt64(r.totalPendingMsg, -1)
		r.deadlineTimer.Stop()
		r.remove()
		replyCliError(r.cli, r.oriSeqno, r.cmd, err)
	}
}

func (r *forwordMsg) dropReply() {
	if r.setReplied() {
		r.deadlineTimer.Stop()
		r.remove()
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
