package raft

import (
	"context"
	"encoding/json"
	"github.com/sniperHW/flyfish/pkg/buffer"
	"github.com/sniperHW/flyfish/pkg/etcd/raft"
	"github.com/sniperHW/flyfish/pkg/etcd/raft/raftpb"
	"github.com/sniperHW/flyfish/pkg/raft/membership"
	"sync"
	"time"
)

type Proposal interface {
	OnError(error)

	/*
	 *  raft层提供一个buff用于序列化用户Proposal,序列完成后需要将buff返回供后续使用
	 */
	Serilize([]byte) []byte

	/*
	 * raft层会按一定的策略将用户提交的proposal合并成一条raft proposal以减少propose在副本之间复制的数量
	 * 合并完成时，用合并后的[]byte作为参数调用最后一个Proposal.OnMergeFinish
	 * 用户可以对合并后的[]byte做进一步的处理（例如，如果[]byte超过一定大小就执行压缩）,如果无需处理则直接把参数返回即可
	 */
	OnMergeFinish([]byte) []byte
}

type ProposalConfChange interface {
	GetType() raftpb.ConfChangeType
	IsPromote() bool
	GetURL() string
	GetClientURL() string
	GetNodeID() uint64
	OnError(error)
	GetProcessID() uint16
}

/*
 *  raft commited的entry,如果GetSnapshotNotify() != nil,表示raft请求应用层传递快照
 *  应用层在apply之后，应该立即建立快照并通过SnapshotNotify将快照传递给raft
 */
type Committed struct {
	Proposals      []Proposal
	Data           []byte
	snapshotNotify *SnapshotNotify
}

func (this Committed) GetSnapshotNotify() *SnapshotNotify {
	return this.snapshotNotify
}

func (rc *RaftInstance) proposeConfChange(proposal ProposalConfChange) {
	pc := membership.ConfChangeContext{
		ConfChangeType: proposal.GetType(),
		IsPromote:      proposal.IsPromote(),
		Url:            proposal.GetURL(),
		ClientUrl:      proposal.GetClientURL(),
		NodeID:         proposal.GetNodeID(),
		ProcessID:      proposal.GetProcessID(),
	}

	buff, _ := json.Marshal(&pc)

	cc := raftpb.ConfChange{
		ID:      rc.reqIDGen.Next(),
		Type:    pc.ConfChangeType,
		NodeID:  pc.NodeID,
		Context: buff,
	}

	ctx, cancel := context.WithTimeout(context.TODO(), 5*time.Second)
	defer cancel()

	var err error

	ch := rc.w.Register(cc.ID)

	if err = rc.node.ProposeConfChange(ctx, cc); err != nil {
		rc.w.Trigger(cc.ID, nil)
	} else {
		select {
		case x := <-ch:
			if nil != x {
				GetSugar().Errorf("ProposeConfChange Error %v", x.(error))
				if x.(error) == raft.ErrProposalDropped {
					err = ErrProposalDropped
				} else {
					err = x.(error)
				}
			}
		case <-ctx.Done():
			err = ErrTimeout
			rc.w.Trigger(cc.ID, nil)
		case <-rc.stopping:
			rc.w.Trigger(cc.ID, nil)
			err = ErrStopped
		}
	}

	if nil != err {
		proposal.OnError(err)
	} else {
		rc.commitC.AppendHighestPriotiryItem(proposal)
	}
}

var proposeBuffPool sync.Pool = sync.Pool{
	New: func() interface{} {
		return make([]byte, 0, 1024*1024)
	},
}

func getProposeBuff() []byte {
	return proposeBuffPool.Get().([]byte)
}

func releaseProposeBuff(b []byte) {
	proposeBuffPool.Put(b[:0])
}

func (rc *RaftInstance) propose(batchProposal []Proposal) {
	t := &raftTask{
		id:    rc.reqIDGen.Next(),
		other: batchProposal,
	}

	buff := getProposeBuff()

	for _, v := range batchProposal {
		buff = v.Serilize(buff)
	}

	buff = batchProposal[len(batchProposal)-1].OnMergeFinish(buff)

	b := make([]byte, 0, len(buff)+8)
	b = buffer.AppendUint64(b, t.id)
	b = buffer.AppendBytes(b, buff)

	releaseProposeBuff(buff)

	rc.proposalMgr.addToDict(t)

	if err := rc.node.Propose(context.TODO(), b); nil != err {
		if nil != rc.proposalMgr.getAndRemoveByID(t.id) {
			for _, v := range batchProposal {
				v.OnError(err)
			}
		}
	}
}

/*
 * 等待来自应用层的proposal(对应一个操作),按策略将多个应用层proposal合并成单个raft proposal
 */
func (rc *RaftInstance) runProposePipeline() {
	rc.waitStop.Add(1)

	go func() {

		defer rc.waitStop.Done()
		localList := []interface{}{}
		var closed bool
		for {

			if localList, closed = rc.proposePipeline.Pop(localList); closed {
				return
			}

			for {

				var batch []Proposal
				if len(localList) <= MaxBatchCount {
					batch = make([]Proposal, 0, len(localList))
				} else {
					batch = make([]Proposal, 0, MaxBatchCount)
				}

				for k, vv := range localList {
					batch = append(batch, vv.(Proposal))
					localList[k] = nil
					if len(batch) == cap(batch) {
						rc.propose(batch)
						if k != len(localList)-1 {
							batch = make([]Proposal, 0, len(localList)-1-k)
						}
					}
				}

				if localList, closed = rc.proposePipeline.PopNoWait(localList); closed || len(localList) == 0 {
					break
				}
			}
		}
	}()
}
