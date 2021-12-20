package raft

import (
	"context"
	"encoding/binary"
	"time"

	"github.com/sniperHW/flyfish/pkg/etcd/raft"
)

//一致性读请求
type LinearizableRead interface {
	OnError(error)
}

func (rc *RaftInstance) checkLinearizableRead() {
	rc.linearizableReadMgr.Lock()
	defer rc.linearizableReadMgr.Unlock()
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
	rc.linearizableReadMgr.Lock()
	defer rc.linearizableReadMgr.Unlock()
	for _, rs := range readStates {
		index := binary.BigEndian.Uint64(rs.RequestCtx)
		v, ok := rc.linearizableReadMgr.dict[index]
		if ok {
			v.ridx = rs.Index
			v.ptrridx = &v.ridx
		}
	}
}

func (rc *RaftInstance) linearizableRead(batchRead []LinearizableRead) {

	t := &raftTask{
		id:    rc.genNextIndex(),
		other: batchRead,
	}

	ctxToSend := make([]byte, 8)
	binary.BigEndian.PutUint64(ctxToSend, t.id)

	rc.linearizableReadMgr.addToDictAndList(t)
	if err := rc.node.ReadIndex(context.TODO(), ctxToSend); nil != err {
		rc.linearizableReadMgr.remove(t)
		for _, v := range batchRead {
			v.OnError(err)
		}
	}
}

func (rc *RaftInstance) runReadPipeline() {

	sleepTime := time.Duration(ProposalFlushInterval)

	go func() {
		for {
			time.Sleep(time.Millisecond * sleepTime)
			//发送信号，触发batch提交
			if rc.readPipeline.ForceAppend(struct{}{}) != nil {
				return
			}
		}
	}()

	go func() {

		defer rc.waitStop.Done()

		localList := []interface{}{}
		closed := false
		batch := make([]LinearizableRead, 0, ReadBatchCount)

		for {

			localList, closed = rc.readPipeline.Pop(localList)

			if closed {
				GetSugar().Info("runReadPipeline break")
				break
			}

			for k, vv := range localList {
				issueRead := false
				switch vv.(type) {
				case struct{}:
					//触发时间到达
					if len(batch) > 0 {
						issueRead = true
					}
				case LinearizableRead:
					batch = append(batch, vv.(LinearizableRead))
					if len(batch) == cap(batch) {
						issueRead = true
					}
				}

				if issueRead {
					rc.linearizableRead(batch)
					batch = make([]LinearizableRead, 0, ReadBatchCount)
				}

				localList[k] = nil
			}
		}
	}()
}
