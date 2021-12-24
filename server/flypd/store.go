package flypd

import (
	"encoding/json"
	"fmt"
	"github.com/sniperHW/flyfish/pkg/buffer"
	"github.com/sniperHW/flyfish/pkg/raft/membership"
	snet "github.com/sniperHW/flyfish/server/net"
	sproto "github.com/sniperHW/flyfish/server/proto"
	"net"
)

type ProposalFlyKvCommited struct {
	*proposalBase
	Set   int
	Node  int
	Store int
	Type  FlyKvStoreStateType
}

func (p *ProposalFlyKvCommited) Serilize(b []byte) []byte {
	b = buffer.AppendByte(b, byte(proposalFlyKvCommited))
	bb, err := json.Marshal(p)
	if nil != err {
		panic(err)
	}
	return buffer.AppendBytes(b, bb)
}

func (p *ProposalFlyKvCommited) apply() {
	//s := p.pd.pState.deployment.sets[p.Set]
	//n := s.nodes[p.Node]

	//st, ok := n.store[p.Store]

	//if p.Type == LearnerStore && ok {
	//	st.Value = FlyKvCommited
	//}

	/*if p.Type == LearnerStore && n.isLearner(p.Store) {
		n.learnerStore[p.Store] = FlyKvStoreState{
			Type:  LearnerStore,
			Value: FlyKvCommited,
		}
	} else if p.Type == VoterStore && n.isVoter(p.Store) {
		n.voterStore[p.Store] = FlyKvStoreState{
			Type:  VoterStore,
			Value: FlyKvCommited,
		}
	} else {
		if n.isLearner(p.Store) {
			delete(n.learnerStore, p.Store)
		} else if n.isVoter(p.Store) {
			delete(n.voterStore, p.Store)
		}
	}*/

	if nil != p.reply {
		p.reply(nil)
	}
}

func (p *pd) replayProposalFlyKvCommited(reader *buffer.BufferReader) error {
	pa := &ProposalFlyKvCommited{}
	if err := json.Unmarshal(reader.GetAll(), &pa); nil != err {
		return err
	} else {
		pa.pd = p
		pa.apply()
		return nil
	}
}

type ProposalAddLearnerStoreToNode struct {
	*proposalBase
	msg *sproto.AddLearnerStoreToNode
}

func (p *ProposalAddLearnerStoreToNode) Serilize(b []byte) []byte {
	b = buffer.AppendByte(b, byte(proposalAddLearnerStoreToNode))
	bb, err := json.Marshal(p.msg)
	if nil != err {
		panic(err)
	}
	return buffer.AppendBytes(b, bb)
}

func (p *ProposalAddLearnerStoreToNode) apply() {
	s := p.pd.pState.deployment.sets[int(p.msg.SetID)]
	n := s.nodes[int(p.msg.NodeID)]

	if _, ok := n.store[int(p.msg.Store)]; !ok {
		n.store[int(p.msg.Store)] = &FlyKvStoreState{
			Type:  LearnerStore,
			Value: FlyKvUnCommit,
		}
		//通告set中flykv添加learner
	}

	if nil != p.reply {
		p.reply(nil)
	}
}

func (p *pd) replayAddLearnerStoreToNode(reader *buffer.BufferReader) error {
	var msg sproto.AddLearnerStoreToNode
	if err := json.Unmarshal(reader.GetAll(), &msg); nil != err {
		return err
	}

	pa := &ProposalAddLearnerStoreToNode{
		proposalBase: &proposalBase{
			pd: p,
		},
		msg: &msg,
	}
	pa.apply()
	return nil
}

type ProposalPromoteLearnerStore struct {
	*proposalBase
	msg *sproto.PromoteLearnerStore
}

func (p *ProposalPromoteLearnerStore) Serilize(b []byte) []byte {
	b = buffer.AppendByte(b, byte(proposalPromoteLearnerStore))
	bb, err := json.Marshal(p.msg)
	if nil != err {
		panic(err)
	}
	return buffer.AppendBytes(b, bb)
}

func (p *ProposalPromoteLearnerStore) apply() {
	s := p.pd.pState.deployment.sets[int(p.msg.SetID)]
	n := s.nodes[int(p.msg.NodeID)]

	if st, ok := n.store[int(p.msg.Store)]; ok && st.Type == LearnerStore && st.Value == FlyKvCommited {
		st.Type = VoterStore
		st.Value = FlyKvUnCommit
		//通告set中flykv promote
	}

	if nil != p.reply {
		p.reply(nil)
	}
}

func (p *pd) replayPromoteLearnerStore(reader *buffer.BufferReader) error {
	var msg sproto.PromoteLearnerStore
	if err := json.Unmarshal(reader.GetAll(), &msg); nil != err {
		return err
	}

	pa := &ProposalPromoteLearnerStore{
		proposalBase: &proposalBase{
			pd: p,
		},
		msg: &msg,
	}
	pa.apply()
	return nil
}

/*
 * 向node添加一个store,新store将以learner身份加入raft集群
 */
func (p *pd) onAddLearnerStoreToNode(from *net.UDPAddr, m *snet.Message) {
	msg := m.Msg.(*sproto.AddLearnerStoreToNode)
	resp := &sproto.AddLearnerStoreToNodeResp{}
	reply, reason := func() (bool, string) {
		if !(msg.Store > 0 && msg.Store <= int32(StorePerSet)) {
			return true, fmt.Sprintf("storeID must between(1,%d)", StorePerSet)
		}

		if nil == p.pState.deployment {
			return true, "must init deployment first"
		}

		s, ok := p.pState.deployment.sets[int(msg.SetID)]
		if !ok {
			return true, "set not found"
		}

		n, ok := s.nodes[int(msg.NodeID)]
		if !ok {
			return true, "node not found"
		}

		st, ok := n.store[int(msg.Store)]
		if ok {
			reason := ""
			if st.Type == VoterStore {
				reason = "store is voter"
			} else if st.Type == RemoveStore {
				reason = "store is removing"
			}
			return true, reason
		}

		learnerNum := 0

		for _, v := range s.nodes {
			if v.isLearner(int(msg.Store)) {
				learnerNum++
			}
		}

		//不允许超过同时存在的learner数量限制
		if learnerNum+1 > membership.MaxLearners {
			return true, "to many leaner"
		}

		return false, ""
	}()

	if reply {
		resp.Ok = (reason == "")
		resp.Reason = reason
		p.udp.SendTo(from, snet.MakeMessage(m.Context, resp))
	} else {
		p.issueProposal(&ProposalAddLearnerStoreToNode{
			proposalBase: &proposalBase{
				pd:    p,
				reply: p.makeReplyFunc(from, m, resp),
			},
			msg: msg,
		})
	}
}

func (p *pd) onPromoteLearnerStore(from *net.UDPAddr, m *snet.Message) {
	msg := m.Msg.(*sproto.PromoteLearnerStore)
	resp := &sproto.PromoteLearnerStoreResp{}

	reply, reason := func() (bool, string) {
		if !(msg.Store > 0 && msg.Store <= int32(StorePerSet)) {
			return true, fmt.Sprintf("storeID must between(1,%d)", StorePerSet)
		}

		if nil == p.pState.deployment {
			return true, "must init deployment first"
		}

		s, ok := p.pState.deployment.sets[int(msg.SetID)]
		if !ok {
			return true, "set not found"
		}

		n, ok := s.nodes[int(msg.NodeID)]
		if !ok {
			return true, "node not found"
		}

		st, ok := n.store[int(msg.Store)]
		if ok {
			if st.Type == VoterStore {
				return true, ""
			} else if st.Type == LearnerStore {
				if st.Value == FlyKvUnCommit {
					return true, "wait for learner to commited by flykv"
				} else {
					return false, ""
				}
			} else {
				return true, "store is removing"
			}
		} else {
			return true, "store is not found"
		}
	}()

	if reply {
		resp.Ok = (reason == "")
		resp.Reason = reason
		p.udp.SendTo(from, snet.MakeMessage(m.Context, resp))
	} else {
		p.issueProposal(&ProposalPromoteLearnerStore{
			proposalBase: &proposalBase{
				pd:    p,
				reply: p.makeReplyFunc(from, m, resp),
			},
			msg: msg,
		})
	}
}
