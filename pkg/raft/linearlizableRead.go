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
	for e := rc.linearizableReadMgr.l.Front(); e != nil; e = rc.linearizableReadMgr.l.Front() {
		v := e.Value.(*raftTask)
		if nil == v.ptrridx {
			break
		} else if rc.appliedIndex < v.ridx {
			break
		} else {
			rc.commitC.AppendHighestPriotiryItem(v.other.([]LinearizableRead))
			delete(rc.linearizableReadMgr.dict, v.id)
			rc.linearizableReadMgr.l.Remove(v.listE)
		}
	}
}

func (rc *RaftInstance) processReadStates(readStates []raft.ReadState) {
	for _, rs := range readStates {
		index := binary.BigEndian.Uint64(rs.RequestCtx)
		v, ok := rc.linearizableReadMgr.dict[index]
		if ok && v.timer.Stop() {
			if rc.appliedIndex < v.ridx {
				v.ridx = rs.Index
				v.ptrridx = &v.ridx
			} else {
				rc.commitC.AppendHighestPriotiryItem(v.other.([]LinearizableRead))
				delete(rc.linearizableReadMgr.dict, v.id)
				rc.linearizableReadMgr.l.Remove(v.listE)
			}
		}
	}
}

func (rc *RaftInstance) linearizableRead(batchRead []LinearizableRead) {

	t := &raftTask{
		id:    rc.reqIDGen.Next(),
		other: batchRead,
	}

	ctxToSend := make([]byte, 8)
	binary.BigEndian.PutUint64(ctxToSend, t.id)

	t.timer = time.AfterFunc(ReadTimeout, func() {
		if nil != rc.linearizableReadMgr.getAndRemoveByID(t.id) {
			for _, v := range batchRead {
				v.OnError(ErrTimeout)
			}
		}
	})

	rc.linearizableReadMgr.addToDictAndList(t)

	if err := rc.node.ReadIndex(context.TODO(), ctxToSend); nil != err {
		if nil != rc.linearizableReadMgr.getAndRemoveByID(t.id) && t.timer.Stop() {
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
