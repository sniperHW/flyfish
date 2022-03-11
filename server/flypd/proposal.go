package flypd

import (
	"encoding/json"
	"errors"
	"github.com/sniperHW/flyfish/pkg/buffer"
)

const (
	proposalInstallDeployment     = 1
	proposalAddNode               = 2
	proposalRemNode               = 4
	proposalBeginSlotTransfer     = 6
	proposalSlotTransOutOk        = 7
	proposalSlotTransInOk         = 8
	proposalAddSet                = 9
	proposalRemSet                = 10
	proposalSetMarkClear          = 11
	proposalInitMeta              = 12
	proposalAddLearnerStoreToNode = 13
	proposalFlyKvCommited         = 14
	proposalPromoteLearnerStore   = 15
	proposalRemoveNodeStore       = 16
	proposalUpdateMeta            = 17
	proposalNop                   = 18
)

type proposalBase struct {
	reply func(error)
}

type applyable interface {
	apply(pd *pd)
}

type replayable interface {
	replay(pd *pd)
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

	if len(pd.pState.deployment.sets) == 0 {
		pd.loadInitDeployment()
	} else {
		//重置slotBalance相关的临时数据
		for _, v := range pd.pState.deployment.sets {
			pd.storeTask = map[uint64]*storeTask{}
			for _, node := range v.nodes {
				for store, state := range node.store {
					if state.Value == FlyKvUnCommit {
						taskID := uint64(node.id)<<32 + uint64(store)
						t := &storeTask{
							node:           node,
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

		for _, v := range pd.pState.SlotTransfer {
			v.notify(pd)
		}
	}

	if pd.pState.Meta.Version == 0 {
		pd.loadInitMeta()
	}
}

func (this *ProposalNop) replay(pd *pd) {

}

func (p *pd) replayProposal(proposal []byte) error {
	reader := buffer.NewReader(proposal)
	proposalType, err := reader.CheckGetByte()
	if nil != err {
		return err
	}

	var r replayable

	unmarshal := func(rr interface{}) (err error) {
		if err = json.Unmarshal(reader.GetAll(), rr); nil == err {
			r = rr.(replayable)
		}
		return
	}

	switch int(proposalType) {
	case proposalInstallDeployment:
		err = unmarshal(&ProposalInstallDeployment{})
	case proposalAddNode:
		err = unmarshal(&ProposalAddNode{})
	case proposalRemNode:
		err = unmarshal(&ProposalRemNode{})
	case proposalBeginSlotTransfer:
		err = unmarshal(&ProposalBeginSlotTransfer{})
	case proposalSlotTransOutOk:
		err = unmarshal(&ProposalSlotTransOutOk{})
	case proposalSlotTransInOk:
		err = unmarshal(&ProposalSlotTransInOk{})
	case proposalAddSet:
		err = unmarshal(&ProposalAddSet{})
	case proposalRemSet:
		err = unmarshal(&ProposalRemSet{})
	case proposalSetMarkClear:
		err = unmarshal(&ProposalSetMarkClear{})
	case proposalInitMeta:
		err = unmarshal(&ProposalInitMeta{})
	case proposalUpdateMeta:
		err = unmarshal(&ProposalUpdateMeta{})
	case proposalFlyKvCommited:
		err = unmarshal(&ProposalFlyKvCommited{})
	case proposalAddLearnerStoreToNode:
		err = unmarshal(&ProposalAddLearnerStoreToNode{})
	case proposalPromoteLearnerStore:
		err = unmarshal(&ProposalPromoteLearnerStore{})
	case proposalRemoveNodeStore:
		err = unmarshal(&ProposalRemoveNodeStore{})
	case proposalNop:
		err = unmarshal(&ProposalNop{})
	default:
		return errors.New("invaild proposal type")
	}

	if nil == err {
		r.replay(p)
	}

	return err
}
