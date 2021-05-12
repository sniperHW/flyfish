package kvnode

import (
	"github.com/sniperHW/flyfish/pkg/net"
	"github.com/sniperHW/flyfish/pkg/net/cs"
	"sync"
)

type conn struct {
	sync.RWMutex
	session    *net.Socket
	pendingCmd map[int64]replyAble
}

func (this *conn) clear() {
	this.Lock()
	defer this.Unlock()
	this.pendingCmd = map[int64]replyAble{}
}

func (this *conn) send(msg *cs.RespMessage) error {
	return this.session.Send(msg)
}

func (this *conn) isClosed() bool {
	return this.session.IsClosed()
}

func (this *conn) addPendingCmd(cmd replyAble) {
	this.Lock()
	defer this.Unlock()
	this.pendingCmd[cmd.getSeqno()] = cmd
}

func (this *conn) removePendingCmdBySeqno(seqno int64) bool {
	this.Lock()
	defer this.Unlock()
	if _, ok := this.pendingCmd[seqno]; ok {
		delete(this.pendingCmd, seqno)
		return true
	} else {
		return false
	}
}

func (this *conn) removePendingCmd(cmd replyAble) bool {
	return this.removePendingCmdBySeqno(cmd.getSeqno())
}

func (this *conn) checkPendingCmd(cmd replyAble) bool {
	this.RLock()
	defer this.RUnlock()
	return nil != this.pendingCmd[cmd.getSeqno()]
}
