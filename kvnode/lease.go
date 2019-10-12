/*
 * sql回写权租约
 */

package kvnode

import (
	"github.com/sniperHW/flyfish/util/str"
	"sync"
	"time"
)

/*
 *   租约时效
 *   租约持有者的超时时效小于非持有者的超时时效
 *   这样，当非持有者发现原租约失效时，原持有者必定已经发现自己的租约失效。
 *
 *   这两个时间必须远大于raft心跳及选举超时。
 */

const (
	leaseTimeout      = 20 * time.Second //租约时效20秒
	leaseOwnerTimeout = 10 * time.Second //当前获得租约的leader的组约时效
	renewTime         = 2 * time.Second  //续约间隔
)

type asynTaskLease struct {
	rn   *raftNode
	term uint64
}

func (this *asynTaskLease) done() {
	if this.rn.lease.update(this.rn, this.rn.id, this.term) {
		var notify leaseNotify
		this.rn.commitC <- notify
	}
}

func (this *asynTaskLease) onError(errno int32) {

}

func (this *asynTaskLease) append2Str(s *str.Str) {
	appendProposal2Str(s, proposal_lease, this.rn.id, this.rn.getTerm())
}

func (this *asynTaskLease) onPorposeTimeout() {

}

type lease struct {
	sync.Mutex
	term      uint64
	owner     int //当前租约持有者
	startTime time.Time
	stop      chan struct{}
}

func (l *lease) getOwner() int {
	l.Lock()
	defer l.Unlock()
	return l.owner
}

func (l *lease) isTimeout() bool {
	l.Lock()
	defer l.Unlock()
	if l.owner == 0 {
		return true
	}
	elapse := time.Now().Sub(l.startTime)
	if elapse > leaseTimeout {
		return false
	}
	return true
}

//返回当前raftNode是否持有租约
func (l *lease) hasLease(rn *raftNode) bool {
	l.Lock()
	defer l.Unlock()
	if l.owner != rn.id || l.term != rn.getTerm() {
		return false
	}
	elapse := time.Now().Sub(l.startTime)
	if elapse > leaseOwnerTimeout {
		return false
	}
	return true
}

//更新租约,返回rn是否获得租约(非续约)
func (l *lease) update(rn *raftNode, id int, term uint64) bool {
	l.Lock()
	defer l.Unlock()
	oldTerm := l.term
	l.term = term
	l.owner = id
	l.startTime = time.Now()
	return rn.id == id && oldTerm != term
}

func (l *lease) wait(stop chan struct{}, second time.Duration) {
	select {
	case <-time.After(second):
	case <-stop:
	}
}

func (l *lease) startLeaseRoutine(rn *raftNode) {
	l.Lock()
	defer l.Unlock()
	if nil == l.stop {
		Infoln("startLeaseRoutine")
		l.stop = make(chan struct{})
		go func() {
			for rn.isLeader() {
				l.Lock()
				stop := l.stop
				owner := l.owner
				l.Unlock()
				if nil == stop {
					return
				}
				if owner != 0 && owner != rn.id {
					if !l.isTimeout() {
						//等待超时
						l.wait(l.stop, time.Second)
						continue
					}
				}
				//续租
				rn.renew()
				l.wait(l.stop, renewTime)
			}
			Infoln("break")
		}()
	}
}

func (l *lease) loseLeaderShip() {
	l.Lock()
	if nil != l.stop {
		stop := l.stop
		l.stop = nil
		l.Unlock()
		stop <- struct{}{}
	} else {
		l.Unlock()
	}
}

func (l *lease) becomeLeader(rn *raftNode) {
	Infoln("becomeLeader")
	l.startLeaseRoutine(rn)
}
