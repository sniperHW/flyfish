package kvnode

import (
	"encoding/binary"
	"github.com/sniperHW/flyfish/pkg/buffer"
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
	beginTime time.Time
	notifyCh  chan error
	store     *kvstore
}

func (this *leaseProposal) Isurgent() bool {
	return true
}

func (this *leaseProposal) OnError(err error) {
	select {
	case this.notifyCh <- err:
	default:
	}
}

func (this *leaseProposal) Serilize(b []byte) []byte {
	this.beginTime = time.Now()
	b = buffer.AppendByte(b, byte(proposal_lease))
	b = buffer.AppendInt32(b, int32(this.store.raftID))
	bb, _ := this.beginTime.MarshalBinary()
	b = buffer.AppendInt32(b, int32(len(bb)))
	b = buffer.AppendBytes(b, bb)
	return b
}

func (this *leaseProposal) OnMergeFinish(b []byte) (ret []byte) {
	if len(b) >= 1024 {
		cb, err := this.store.proposalCompressor.Compress(b)
		if nil != err {
			ret = buffer.AppendByte(b, byte(0))
		} else {
			b = b[:0]
			b = buffer.AppendBytes(b, cb)
			ret = buffer.AppendByte(b, byte(1))
		}
	} else {
		ret = buffer.AppendByte(b, byte(0))
	}
	return
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
	sync.Mutex
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
				l.Lock()
				owner := l.owner
				beginTime := l.beginTime
				l.Unlock()

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
	l.beginTime = time.Time(beginTime)
	l.owner = owner
	l.Unlock()
}

func (l *lease) hasLease() bool {
	l.Lock()
	defer l.Unlock()
	if !l.store.isLeader() {
		return false
	} else if l.owner != l.store.raftID || time.Now().Sub(l.beginTime) >= leaseOwnerTimeout {
		return false
	} else {
		return true
	}
}

func (l *lease) snapshot(b []byte) []byte {
	l.Lock()
	defer l.Unlock()
	ll := len(b)
	b = buffer.AppendInt32(b, 0) //占位符
	b = buffer.AppendByte(b, byte(proposal_lease))
	b = buffer.AppendInt32(b, int32(l.owner))
	bb, _ := l.beginTime.MarshalBinary()
	b = buffer.AppendInt32(b, int32(len(bb)))
	b = buffer.AppendBytes(b, bb)
	b = append(b, byte(0)) //写入无压缩标记

	binary.BigEndian.PutUint32(b[ll:ll+4], uint32(len(b)-ll-4))

	return b
}
