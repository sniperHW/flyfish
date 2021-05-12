package raft

import (
	"context"
	"encoding/binary"
	"go.etcd.io/etcd/raft"
	"time"
)

//一致性读请求
type LinearizableRead interface {
	OnError(error)
}

func (rc *RaftNode) checkLinearizableRead() {
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
			rc.linearizableReadMgr.l.Remove(v.lelement)
		}
	}
}

func (rc *RaftNode) processReadStates(readStates []raft.ReadState) {
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

func (rc *RaftNode) linearizableRead(batchRead []LinearizableRead) {

	t := &raftTask{
		id:    rc.genNextIndex(),
		other: batchRead,
		onTimeout: func() {
			for _, v := range batchRead {
				v.OnError(ERR_TIMEOUT)
			}
		},
		onLeaderDemote: func() {
			for _, v := range batchRead {
				v.OnError(ERR_LEADER_DEMOTE)
			}
		},
	}

	ctxToSend := make([]byte, 8)
	binary.BigEndian.PutUint64(ctxToSend, t.id)

	rc.linearizableReadMgr.insert(t)

	begin := time.Now()

	cctx, cancel := context.WithTimeout(context.Background(), ReadTimeout)
	err := rc.node.ReadIndex(cctx, ctxToSend)
	cancel()
	if nil != err {
		rc.linearizableReadMgr.remove(t)

		if err == cctx.Err() {
			err = ERR_TIMEOUT
		}

		for _, v := range batchRead {
			v.OnError(err)
		}

	} else {
		now := time.Now()
		elapse := now.Sub(begin)
		rc.linearizableReadMgr.Lock()
		t.deadline = now.Add(ReadTimeout - elapse)
		rc.linearizableReadMgr.Unlock()
	}
}

func (rc *RaftNode) runReadPipeline() {

	sleepTime := time.Duration(ProposalFlushInterval)

	go func() {
		for {
			time.Sleep(time.Millisecond * sleepTime)
			//发送信号，触发batch提交
			if rc.readPipeline.ForceAppend(nil) != nil {
				return
			}
		}
	}()

	go func() {

		defer rc.waitStop.Done()

		localList := make([]interface{}, 0, 200)
		closed := false
		batch := make([]LinearizableRead, 0, ReadBatchCount)

		for {

			localList, closed = rc.readPipeline.Pop(localList)

			if closed {
				GetSugar().Info("runReadPipeline break")
				break
			}

			for _, vv := range localList {
				issueRead := false
				if nil == vv {
					//触发时间到达
					if len(batch) > 0 {
						issueRead = true
					}
				} else {
					batch = append(batch, vv.(LinearizableRead))
					if len(batch) == cap(batch) {
						issueRead = true
					}
				}

				if issueRead {
					rc.linearizableRead(batch)
					batch = make([]LinearizableRead, 0, ReadBatchCount)
				}
			}
		}
	}()
}
