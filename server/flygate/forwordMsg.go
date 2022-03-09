package flygate

import (
	"container/list"
	"github.com/sniperHW/flyfish/errcode"
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
	replyer         *replyer
	replied         bool
	totalPendingReq *int64
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
		r.msgMap = msgMap
	}

	if nil != l {
		r.removeList()
		r.l = l
		r.listElement = l.PushBack(r)
	}

}

func (r *forwordMsg) setReplied() bool {
	if r.replied {
		return false
	} else {
		r.replied = true
		return true
	}
}

func (r *forwordMsg) reply(b []byte) {
	if r.setReplied() {
		r.deadlineTimer.Stop()
		r.remove()
		r.replyer.reply(b)
	}
}

func (r *forwordMsg) replyErr(err errcode.Error) {
	if r.setReplied() {
		r.deadlineTimer.Stop()
		r.remove()
		r.replyer.replyErr(r.oriSeqno, r.cmd, err)
	}
}

func (r *forwordMsg) dropReply() {
	if r.setReplied() {
		r.deadlineTimer.Stop()
		r.remove()
		r.replyer.dropReply()
	}
}
