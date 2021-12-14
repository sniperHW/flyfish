package flykv

import (
	"sync"
	"sync/atomic"
	"time"
)

const (
	leaseTimeout      = 30 * time.Second
	leaseOwnerTimeout = 15 * time.Second
	renewTime         = 5 * time.Second
)

type leaseProposal struct {
	proposalBase
	beginTime time.Time
	notifyCh  chan error
	store     *kvstore
}

func (this *leaseProposal) Isurgent() bool {
	return true
}

func (this *leaseProposal) OnError(err error) {
	GetSugar().Errorf("leaseProposal error:%v", err)
	select {
	case this.notifyCh <- err:
	default:
	}
}

func (this *leaseProposal) Serilize(b []byte) []byte {
	this.beginTime = time.Now()
	return serilizeLease(b, this.store.raftID, this.beginTime)
}

func (this *leaseProposal) apply() {
	this.store.lease.update(this.store.raftID, this.beginTime)
	/*
	 *  如果proposal被commited的延时过长就可能发生lease超时的情况
	 */
	if time.Now().Sub(this.beginTime) < leaseOwnerTimeout {
		this.store.gotLease()
	}

	select {
	case this.notifyCh <- nil:
	default:
	}
}

type lease struct {
	sync.RWMutex
	store        *kvstore
	owner        int
	beginTime    time.Time
	nextRenew    time.Time
	leaderWaitCh chan struct{}
	stoped       int32
}

func newLease(store *kvstore) *lease {
	l := &lease{
		store:        store,
		leaderWaitCh: make(chan struct{}, 1),
	}

	go func() {
		for 0 == atomic.LoadInt32(&l.stoped) {
			<-l.leaderWaitCh //等待成为leader
			if 0 == atomic.LoadInt32(&l.stoped) {
				l.RLock()
				owner := l.owner
				beginTime := l.beginTime
				l.RUnlock()

				if owner != 0 && owner != l.store.raftID {
					//之前的lease不是自己持有，且尚未过期，需要等待过期之后才能申请lease
					now := time.Now()
					deadline := beginTime.Add(leaseTimeout)
					if deadline.After(now) {
						time.Sleep(deadline.Sub(now))
					}
				}

				for 0 == atomic.LoadInt32(&l.stoped) && l.store.isLeader() {
					proposal := &leaseProposal{
						store:     l.store,
						notifyCh:  make(chan error, 1),
						beginTime: time.Now(),
					}

					if nil != l.store.rn.IssueProposal(proposal) {
						return
					}

					if r := <-proposal.notifyCh; nil == r {
						elapse := time.Now().Sub(l.beginTime)
						if elapse < renewTime {
							time.Sleep(renewTime - elapse)
						}
					} else {
						GetSugar().Errorf("lease error:%v", r)
					}
				}
			}
		}
	}()
	return l
}

func (l *lease) becomeLeader() {
	select {
	case l.leaderWaitCh <- struct{}{}:
	default:
	}
}

func (l *lease) update(owner int, beginTime time.Time) {
	l.Lock()
	defer l.Unlock()
	l.beginTime = time.Time(beginTime)
	l.owner = owner
}

func (l *lease) hasLease() bool {
	l.RLock()
	defer l.RUnlock()
	if !l.store.isLeader() {
		return false
	} else if l.owner != l.store.raftID || time.Now().Sub(l.beginTime) >= leaseOwnerTimeout {
		return false
	} else {
		return true
	}
}

func (l *lease) snapshot(b []byte) []byte {
	//lease.update在主线程执行，snapshot也在主线程执行，无需加锁
	return serilizeLease(b, l.owner, l.beginTime)
}

func (l *lease) stop() {
	atomic.StoreInt32(&l.stoped, 1)
}
