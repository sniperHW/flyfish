package kvnode

import (
	"github.com/sniperHW/kendynet"
	"sync"
	"time"
)

type cliConn struct {
	sync.RWMutex
	session  kendynet.StreamSession
	replyers map[int64]*replyer
}

func (this *cliConn) send(o interface{}) error {
	return this.session.Send(o)
}

func (this *cliConn) isClosed() bool {
	return this.session.IsClosed()
}

func (this *cliConn) close(reason string, timeout time.Duration) {
	return session.Close()
}

func (this *cliConn) addReplyer(replyer *replyer) {
	this.Lock()
	defer this.Unlock()
	this.replyers[replyer.seqno] = replyer
}

func (this *cliConn) removeReplyerBySeqno(seqno int) bool {
	this.Lock()
	defer this.Unlock()
	if _, ok := this.replyers[seqno]; ok {
		delete(this.replyers, seqno)
		return true
	} else {
		return false
	}
}

func (this *cliConn) removeReplyer(replyer *replyer) bool {
	this.removeReplyerBySeqno(replyer.seqno)
}

func (this *cliConn) checkReplyer(replyer *replyer) bool {
	this.RLock()
	defer this.RUnlock()
	return nil != this.replyers[replyer.seqno]
}
