package kvnode

import (
	"github.com/sniperHW/flyfish/errcode"
	//"github.com/sniperHW/flyfish/util/str"
	"go.etcd.io/etcd/raft/raftpb"
)

type asynTaskConfChange struct {
	nodeid     uint64
	changeType raftpb.ConfChangeType
	url        string
	doneCB     func()
	errorCB    func(errno int32)
}

func (this *asynTaskConfChange) done() {
	if nil != this.doneCB {
		this.doneCB()
	}
}

func (this *asynTaskConfChange) onError(errno int32) {
	if nil != this.errorCB {
		this.errorCB(errno)
	}
}

/*func (this *asynTaskConfChange) append2Str(s *str.Str) {

}*/

func (this *asynTaskConfChange) onPorposeTimeout() {
	if nil != this.errorCB {
		this.errorCB(errcode.ERR_TIMEOUT)
	}
}
