package raft

import (
	"context"
	"encoding/json"
	"github.com/sniperHW/flyfish/pkg/buffer"
	"github.com/sniperHW/flyfish/pkg/etcd/raft/raftpb"
	"sync"
	"time"
)

type Proposal interface {
	/*
	 *  true: 立即触发propose
	 *  false: 累积proposal,符合条件时触发一条合并的propose
	 */
	Isurgent() bool

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

type proposalConfChange struct {
	Index          uint64
	ConfChangeType raftpb.ConfChangeType
	IsPromote      bool
	Url            string //for add
	NodeID         uint64
}

type ProposalConfChange interface {
	GetType() raftpb.ConfChangeType
	IsPromote() bool
	GetURL() string
	GetNodeID() uint64
	OnError(error)
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

	t := &raftTask{
		id:    rc.genNextIndex(),
		other: proposal,
	}

	pc := proposalConfChange{
		Index:          t.id,
		ConfChangeType: proposal.GetType(),
		IsPromote:      proposal.IsPromote(),
		Url:            proposal.GetURL(),
		NodeID:         proposal.GetNodeID(),
	}

	buff, _ := json.Marshal(&pc)

	cfChange := raftpb.ConfChange{
		ID:      pc.Index,
		Type:    pc.ConfChangeType,
		NodeID:  pc.NodeID,
		Context: buff,
	}

	rc.confChangeMgr.addToDict(t)
	if err := rc.node.ProposeConfChange(context.TODO(), cfChange); nil != err {
		rc.confChangeMgr.remove(t)
		proposal.OnError(err)
	}
}

func (rc *RaftInstance) runConfChange() {
	go func() {
		defer rc.waitStop.Done()
		localList := []interface{}{}
		closed := false
		for {

			localList, closed = rc.confChangeC.Pop(localList)

			if closed {
				GetSugar().Info("runConfChange break")
				break
			}

			for k, vv := range localList {
				rc.proposeConfChange(vv.(ProposalConfChange))
				localList[k] = nil
			}
		}
	}()
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
		id:    rc.genNextIndex(),
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
		GetSugar().Errorf("proposalError %v", err)
		rc.proposalMgr.remove(t)
		for _, v := range batchProposal {
			v.OnError(err)
		}
	}
}

/*
 * 等待来自应用层的proposal(对应一个操作),按策略将多个应用层proposal合并成单个raft proposal
 */
func (rc *RaftInstance) runProposePipeline() {

	sleepTime := time.Duration(ProposalFlushInterval)

	go func() {
		for {
			time.Sleep(time.Millisecond * sleepTime)
			//发送信号，触发batch提交
			if rc.proposePipeline.ForceAppend(struct{}{}) != nil {
				return
			}
		}
	}()

	go func() {

		defer rc.waitStop.Done()

		localList := []interface{}{}
		closed := false
		batch := make([]Proposal, 0, ProposalBatchCount)

		for {

			localList, closed = rc.proposePipeline.Pop(localList)

			if closed {
				GetSugar().Info("runProposePipeline break")
				break
			}

			for k, vv := range localList {
				issuePropose := false

				switch vv.(type) {
				case struct{}:
					//触发时间到达
					if len(batch) > 0 {
						issuePropose = true
					}
				case Proposal:
					batch = append(batch, vv.(Proposal))
					if vv.(Proposal).Isurgent() || len(batch) == cap(batch) {
						issuePropose = true
					}
				}

				if issuePropose {
					rc.propose(batch)
					batch = make([]Proposal, 0, ProposalBatchCount)
				}

				localList[k] = nil
			}

		}
	}()
}
