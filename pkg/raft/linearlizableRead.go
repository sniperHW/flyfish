package raft

import (
	"context"
	"encoding/binary"
	"github.com/sniperHW/flyfish/pkg/etcd/raft"
	"time"
)

//一致性读请求
type LinearizableRead interface {
	OnError(error)
}

func (rc *RaftInstance) checkLinearizableRead() {
	for e := rc.pendingReadMgr.l.Front(); e != nil; e = rc.pendingReadMgr.l.Front() {
		v := e.Value.(*pendingRead)
		if nil == v.ridx || rc.appliedIndex < (*v.ridx) {
			break
		} else {
			rc.commitC.AppendHighestPriotiryItem(v.reads)
			delete(rc.pendingReadMgr.dict, v.id)
			rc.pendingReadMgr.l.Remove(v.listE)
		}
	}
}

func (rc *RaftInstance) processReadStates(readStates []raft.ReadState) {
	for _, rs := range readStates {
		index := binary.BigEndian.Uint64(rs.RequestCtx)
		v, ok := rc.pendingReadMgr.dict[index]
		if ok && v.timer.Stop() {
			if rc.appliedIndex < rs.Index {
				v.ridx = new(uint64)
				*v.ridx = rs.Index
			} else {
				rc.commitC.AppendHighestPriotiryItem(v.reads)
				delete(rc.pendingReadMgr.dict, v.id)
				rc.pendingReadMgr.l.Remove(v.listE)
			}
		}
	}
}

func (rc *RaftInstance) linearizableRead(batchRead []LinearizableRead) {

	t := &pendingRead{
		id:    rc.reqIDGen.Next(),
		reads: batchRead,
	}

	ctxToSend := make([]byte, 8)
	binary.BigEndian.PutUint64(ctxToSend, t.id)

	t.timer = time.AfterFunc(ReadTimeout, func() {
		if nil != rc.pendingReadMgr.remove(t.id) {
			for _, v := range batchRead {
				v.OnError(ErrTimeout)
			}
		}
	})

	rc.pendingReadMgr.add(t)

	if err := rc.node.ReadIndex(context.TODO(), ctxToSend); nil != err {
		if nil != rc.pendingReadMgr.remove(t.id) && t.timer.Stop() {
			for _, v := range batchRead {
				v.OnError(err)
			}
		}
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

			batch := make([]LinearizableRead, 0, len(localList))

			for k, vv := range localList {
				batch = append(batch, vv.(LinearizableRead))
				localList[k] = nil
			}

			rc.linearizableRead(batch)
		}
	}()
}
