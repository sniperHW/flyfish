package raft

import (
	"bytes"
	"context"
	"encoding/binary"
	"github.com/sniperHW/flyfish/pkg/etcd/raft"
	"time"
)

//一致性读请求
type LinearizableRead interface {
	OnError(error)
}

func (rc *RaftInstance) linearizableReadLoop() {
	var rs raft.ReadState

	for {
		ctxToSend := make([]byte, 8)
		id1 := rc.reqIDGen.Next()
		binary.BigEndian.PutUint64(ctxToSend, id1)
		leaderChangedNotifier := rc.LeaderChangedNotify()
		select {
		case <-leaderChangedNotifier:
			continue
		case <-rc.readwaitc:
		case <-rc.stopping:
			return
		}

		// as a single loop is can unlock multiple reads, it is not very useful
		// to propagate the trace from Txn or Range.
		nextnr := newNotifier()

		rc.readMu.Lock()
		nr := rc.readNotifier
		rc.readNotifier = nextnr
		rc.readMu.Unlock()

		cctx, cancel := context.WithTimeout(context.Background(), rc.option.ReadTimeout)
		if err := rc.node.ReadIndex(cctx, ctxToSend); err != nil {
			cancel()
			if err == raft.ErrStopped {
				return
			}
			nr.notify(err)
			continue
		}
		cancel()

		var (
			timeout bool
			done    bool
		)
		for !timeout && !done {
			select {
			case rs = <-rc.readStateC:
				done = bytes.Equal(rs.RequestCtx, ctxToSend)
			case <-leaderChangedNotifier:
				timeout = true
				// return a retryable error.
				nr.notify(ErrLeaderChange)
			case <-time.After(rc.option.ReadTimeout):
				nr.notify(ErrTimeout)
				timeout = true
			case <-rc.stopping:
				return
			}
		}
		if !done {
			continue
		}

		index := rs.Index

		ai := rc.GetApplyIndex()
		if ai < index {
			select {
			case <-rc.applyWait.Wait(index):
			case <-rc.stopping:
				return
			}
		}
		// unblock all l-reads requested at indices before rs.Index
		nr.notify(nil)
	}
}

func (rc *RaftInstance) linearizableReadNotify() error {

	rc.readMu.RLock()
	nc := rc.readNotifier
	rc.readMu.RUnlock()

	// signal linearizable loop for current notify if it hasn't been already
	select {
	case rc.readwaitc <- struct{}{}:
	default:
	}

	// wait for read state notification
	select {
	case <-nc.c:
		return nc.err
	case <-rc.stopping:
		return ErrStopped
	}
}

func (rc *RaftInstance) runReadPipeline() {
	rc.waitStop.Add(1)
	go func() {
		defer rc.waitStop.Done()
		localList := []interface{}{}
		closed := false
		for {
			if localList, closed = rc.readPipeline.Pop(localList); closed {
				return
			}
			err := rc.linearizableReadNotify()
			batch := make([]LinearizableRead, 0, len(localList))
			for k, vv := range localList {
				batch = append(batch, vv.(LinearizableRead))
				localList[k] = nil
			}

			go func() {
				if nil == err {
					rc.commitC.AppendHighestPriotiryItem(batch)
				} else {
					for _, v := range batch {
						v.OnError(err)
					}
				}
			}()
		}
	}()
}
