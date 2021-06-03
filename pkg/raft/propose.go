package raft

import (
	"context"
	"time"

	"github.com/sniperHW/flyfish/pkg/buffer"
	"go.etcd.io/etcd/raft/raftpb"
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

type ProposalConfChangeBase struct {
	ConfChangeType raftpb.ConfChangeType
	Url            string //for add
	NodeID         uint64
}

type ProposalConfChange interface {
	GetType() raftpb.ConfChangeType
	GetUrl() string
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

func (rc *RaftNode) proposeConfChange(proposal ProposalConfChange) {

	t := &raftTask{
		id:    rc.genNextIndex(),
		other: proposal,
		onLeaderDemote: func() {
			proposal.OnError(ERR_LEADER_DEMOTE)
		},
	}

	buff := make([]byte, 0, 8+len(proposal.GetUrl()))
	buff = buffer.AppendUint64(buff, t.id)
	buff = buffer.AppendString(buff, proposal.GetUrl())

	cfChange := raftpb.ConfChange{
		ID:      t.id,
		Type:    proposal.GetType(),
		NodeID:  proposal.GetNodeID(),
		Context: buff,
	}

	rc.confChangeMgr.insert(t)
	if err := rc.node.ProposeConfChange(context.TODO(), cfChange); nil != err {
		rc.confChangeMgr.remove(t)
		proposal.OnError(err)
	}
}

func (rc *RaftNode) runConfChange() {
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

func (rc *RaftNode) propose(batchProposal []Proposal) {
	t := &raftTask{
		id:    rc.genNextIndex(),
		other: batchProposal,
		onLeaderDemote: func() {
			for _, v := range batchProposal {
				v.OnError(ERR_LEADER_DEMOTE)
			}
		},
	}

	buff := make([]byte, 0, 4096)

	for _, v := range batchProposal {
		buff = v.Serilize(buff)
	}

	buff = batchProposal[len(batchProposal)-1].OnMergeFinish(buff)

	b := make([]byte, 0, len(buff)+8)
	b = buffer.AppendUint64(b, t.id)
	b = buffer.AppendBytes(b, buff)

	rc.proposalMgr.insert(t)
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
func (rc *RaftNode) runProposePipeline() {

	sleepTime := time.Duration(ProposalFlushInterval)

	go func() {
		for {
			time.Sleep(time.Millisecond * sleepTime)
			//发送信号，触发batch提交
			if rc.proposePipeline.ForceAppend(nil) != nil {
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
				if nil == vv {
					//触发时间到达
					if len(batch) > 0 {
						issuePropose = true
					}
				} else {
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
