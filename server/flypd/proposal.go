package flypd

import (
	"encoding/json"
	//"errors"
	"github.com/sniperHW/flyfish/pkg/buffer"
)

const (
	proposalInstallDeployment = 1
	proposalAddNode           = 2
	proposalRemNode           = 4
	//proposalBeginSlotTransfer     = 6
	proposalSlotTransOutOk = 7
	proposalSlotTransInOk  = 8
	proposalAddSet         = 9
	proposalRemSet         = 10
	proposalSetMarkClear   = 11
	proposalInitMeta       = 12
	//proposalAddLearnerStoreToNode = 13
	proposalFlyKvCommited = 14
	//proposalPromoteLearnerStore   = 15
	//proposalRemoveNodeStore       = 16
	proposalUpdateMeta = 17
	proposalNop        = 18
)

type proposalBase struct {
	reply func(error)
}

type applyable interface {
	apply(pd *pd)
}

func (p proposalBase) OnError(err error) {
	if nil != p.reply {
		p.reply(err)
	}
}

func (p proposalBase) OnMergeFinish(b []byte) []byte {
	return b
}

func serilizeProposal(b []byte, tt int, p interface{}) []byte {
	b = buffer.AppendByte(b, byte(tt))
	bb, err := json.Marshal(p)
	if nil != err {
		panic(err)
	}
	b = buffer.AppendInt32(b, int32(len(bb)))
	return buffer.AppendBytes(b, bb)
}

type ProposalNop struct {
	proposalBase
}

func (this *ProposalNop) OnError(err error) {
	GetSugar().Errorf("proposalNop error:%v", err)
}

func (this *ProposalNop) Serilize(b []byte) []byte {
	return serilizeProposal(b, proposalNop, this)
}

func (this *ProposalNop) apply(pd *pd) {
	GetSugar().Infof("ProposalNop.apply")

	if pd.isLeader() {
		if pd.pState.Meta.Version == 0 {
			pd.loadInitMeta()
		}

		if len(pd.pState.deployment.sets) == 0 {
			pd.loadInitDeployment()
		} else {
			//重置slotBalance相关的临时数据
			for _, v := range pd.pState.deployment.sets {
				for _, node := range v.nodes {
					if node.removing || !node.storeIsOk() {
						pd.pendingNodes[node.id] = node
					}
				}
			}

			for _, v := range pd.pendingNodes {
				for store, state := range v.store {
					if state.Type != VoterStore && state.Value == FlyKvUnCommit {
						if state.Value == FlyKvUnCommit {
							taskID := uint64(v.id)<<32 + uint64(store)
							t := &storeTask{
								node:           v,
								pd:             pd,
								store:          store,
								storeStateType: state.Type,
							}
							pd.storeTask[taskID] = t
							t.notifyFlyKv()
						}
					}
				}
			}

			pd.slotBalance()
		}
	}
}
