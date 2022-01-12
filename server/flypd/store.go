package flypd

import (
	"errors"
	"fmt"
	"github.com/sniperHW/flyfish/pkg/raft/membership"
	snet "github.com/sniperHW/flyfish/server/net"
	sproto "github.com/sniperHW/flyfish/server/proto"
	"net"
	"time"
)

type storeTask struct {
	node           *kvnode
	store          int
	storeStateType FlyKvStoreStateType
	timer          *time.Timer
	pd             *pd
}

func (st *storeTask) notifyFlyKv() {
	if !st.pd.isLeader() {
		return
	}

	taskID := uint64(st.node.id)<<32 + uint64(st.store)
	t, ok := st.pd.storeTask[taskID]
	if ok && t == st {
		if store, ok := st.node.store[st.store]; ok && store.Type == st.storeStateType && store.Value == FlyKvUnCommit {
			msg := &sproto.NotifyNodeStoreOp{
				NodeID: int32(st.node.id),
				Store:  int32(st.store),
				Op:     int32(st.storeStateType),
			}

			if st.storeStateType == LearnerStore {
				msg.Host = st.node.host
				msg.RaftPort = int32(st.node.raftPort)
			}

			var remotes []string

			for _, v := range st.node.set.nodes {
				if v != st.node {
					if _, ok := v.store[st.store]; ok {
						remotes = append(remotes, fmt.Sprintf("%s:%d", v.host, v.servicePort))
					}
				}
			}

			go func() {
				context := snet.MakeUniqueContext()
				resp := snet.UdpCall(remotes, snet.MakeMessage(context, msg), time.Second*3, func(respCh chan interface{}, r interface{}) {
					if m, ok := r.(*snet.Message); ok && context == m.Context {
						if resp, ok := m.Msg.(*sproto.NodeStoreOpOk); ok {
							select {
							case respCh <- resp:
							default:
							}
						}
					}
				})

				if nil != resp {
					st.pd.issueProposal(&ProposalFlyKvCommited{
						Set:   st.node.set.id,
						Node:  st.node.id,
						Store: st.store,
						Type:  st.storeStateType,
					})
				} else {
					st.timer = time.AfterFunc(time.Second*3, func() {
						st.pd.mainque.AppendHighestPriotiryItem(st.notifyFlyKv)
					})
				}
			}()
		}
	}
}

func (p *pd) startStoreNotifyTask(node *kvnode, store int, storeStateType FlyKvStoreStateType) {
	taskID := uint64(node.id)<<32 + uint64(store)
	t, ok := p.storeTask[taskID]
	if !ok {
		t = &storeTask{
			node:           node,
			pd:             p,
			store:          store,
			storeStateType: storeStateType,
		}
		p.storeTask[taskID] = t
		t.notifyFlyKv()
	}
}

type ProposalFlyKvCommited struct {
	proposalBase
	Set   int
	Node  int
	Store int
	Type  FlyKvStoreStateType
}

func (p *ProposalFlyKvCommited) Serilize(b []byte) []byte {
	return serilizeProposal(b, proposalFlyKvCommited, p)
}

func (p *ProposalFlyKvCommited) apply(pd *pd) {

	GetSugar().Infof("ProposalFlyKvCommited apply %v", p.Type)

	s := pd.pState.deployment.sets[p.Set]
	n := s.nodes[p.Node]

	st := n.store[p.Store]

	switch p.Type {
	case LearnerStore, VoterStore:
		st.Value = FlyKvCommited
	case RemoveStore:
		delete(n.store, p.Store)
	}

	pd.pState.deployment.version++

	taskID := uint64(p.Node)<<32 + uint64(p.Store)

	_, ok := pd.storeTask[taskID]
	if ok {
		delete(pd.storeTask, taskID)
	}
}

func (p *ProposalFlyKvCommited) replay(pd *pd) {
	p.apply(pd)
}

type ProposalAddLearnerStoreToNode struct {
	proposalBase
	Msg *sproto.AddLearnerStoreToNode
}

func (p *ProposalAddLearnerStoreToNode) Serilize(b []byte) []byte {
	return serilizeProposal(b, proposalAddLearnerStoreToNode, p)
}

func (p *ProposalAddLearnerStoreToNode) apply(pd *pd) {

	s := pd.pState.deployment.sets[int(p.Msg.SetID)]

	n := s.nodes[int(p.Msg.NodeID)]

	err := func() error {

		if _, ok := n.store[int(p.Msg.Store)]; ok {
			return errors.New("store already exists")
		}

		learnerNum := 0

		for _, v := range s.nodes {
			if v.isLearner(int(p.Msg.Store)) {
				learnerNum++
			}
		}

		//不允许超过同时存在的learner数量限制
		if learnerNum+1 > membership.MaxLearners {
			return errors.New("to many learner")
		}

		return nil

	}()

	if nil == err {
		n.store[int(p.Msg.Store)] = &FlyKvStoreState{
			Type:  LearnerStore,
			Value: FlyKvUnCommit,
		}
		//通告set中flykv添加learner
		pd.startStoreNotifyTask(n, int(p.Msg.Store), LearnerStore)
	}

	if nil != p.reply {
		p.reply(err)
	}
}

func (p *ProposalAddLearnerStoreToNode) replay(pd *pd) {
	p.apply(pd)
}

type ProposalPromoteLearnerStore struct {
	proposalBase
	Msg *sproto.PromoteLearnerStore
}

func (p *ProposalPromoteLearnerStore) Serilize(b []byte) []byte {
	return serilizeProposal(b, proposalPromoteLearnerStore, p)
}

func (p *ProposalPromoteLearnerStore) apply(pd *pd) {

	var st *FlyKvStoreState
	var n *kvnode

	err := func() error {
		s, ok := pd.pState.deployment.sets[int(p.Msg.SetID)]
		if !ok {
			return errors.New("set not found")
		}

		n, ok = s.nodes[int(p.Msg.NodeID)]
		if !ok {
			return errors.New("node not found")
		}

		st, ok = n.store[int(p.Msg.Store)]
		if ok {
			if st.Type == VoterStore {
				return errors.New("store is already a voter")
			} else if st.Type == LearnerStore {
				if st.Value == FlyKvUnCommit {
					return errors.New("wait for commit by flykv")
				} else {
					return nil
				}
			} else {
				return errors.New("store is removing")
			}
		} else {
			return errors.New("store is not found")
		}
	}()

	if nil == err {
		st.Type = VoterStore
		st.Value = FlyKvUnCommit
		//通告set中flykv promote
		pd.startStoreNotifyTask(n, int(p.Msg.Store), VoterStore)
	}

	if nil != p.reply {
		p.reply(nil)
	}
}

func (p *ProposalPromoteLearnerStore) replay(pd *pd) {
	p.apply(pd)
}

type ProposalRemoveNodeStore struct {
	proposalBase
	Msg *sproto.RemoveNodeStore
}

func (p *ProposalRemoveNodeStore) Serilize(b []byte) []byte {
	return serilizeProposal(b, proposalRemoveNodeStore, p)
}

func (p *ProposalRemoveNodeStore) apply(pd *pd) {

	GetSugar().Infof("ProposalRemoveNodeStore apply")

	s := pd.pState.deployment.sets[int(p.Msg.SetID)]
	n := s.nodes[int(p.Msg.NodeID)]
	store := n.store[int(p.Msg.Store)]

	err := func() error {
		if nil == store {
			return errors.New("store not exists")
		} else if store.Type == RemoveStore {
			return errors.New("store is removing")
		} else if store.Value == FlyKvUnCommit {
			return errors.New("wait for previous op commit by flykv")
		} else {
			return nil
		}
	}()

	if nil == err {
		store.Type = RemoveStore
		store.Value = FlyKvUnCommit
		pd.startStoreNotifyTask(n, int(p.Msg.Store), RemoveStore)
	}

	if nil != p.reply {
		p.reply(err)
	}

}

func (p *ProposalRemoveNodeStore) replay(pd *pd) {
	p.apply(pd)
}

/*
 * 向node添加一个store,新store将以learner身份加入raft集群
 */
func (p *pd) onAddLearnerStoreToNode(from *net.UDPAddr, m *snet.Message) {
	msg := m.Msg.(*sproto.AddLearnerStoreToNode)
	resp := &sproto.AddLearnerStoreToNodeResp{}
	err := func() error {
		if !(msg.Store > 0 && msg.Store <= int32(StorePerSet)) {
			return errors.New(fmt.Sprintf("storeID must between(1,%d)", StorePerSet))
		}

		if nil == p.pState.deployment {
			return errors.New("must init deployment first")
		}

		s, ok := p.pState.deployment.sets[int(msg.SetID)]
		if !ok {
			return errors.New("set not found")
		}

		n, ok := s.nodes[int(msg.NodeID)]
		if !ok {
			return errors.New("node not found")
		}

		st, ok := n.store[int(msg.Store)]
		if ok {
			if st.Type == LearnerStore {
				if st.Value == FlyKvUnCommit {
					return errors.New("learner wait for commit by flykv")
				} else {
					return errors.New("learner store already exists")
				}
			} else if st.Type == VoterStore {
				if st.Value == FlyKvUnCommit {
					return errors.New("voter store wait for commit by flykv")
				} else {
					return errors.New("store is voter")
				}
			} else {
				return errors.New("remove store wait for commit by flykv")
			}
		}

		learnerNum := 0

		for _, v := range s.nodes {
			if v.isLearner(int(msg.Store)) {
				learnerNum++
			}
		}

		//不允许超过同时存在的learner数量限制
		if learnerNum+1 > membership.MaxLearners {
			return errors.New("to many learner")
		}

		return nil
	}()

	if nil != err {
		resp.Ok = false
		resp.Reason = err.Error()
		p.udp.SendTo(from, snet.MakeMessage(m.Context, resp))
	} else {
		p.issueProposal(&ProposalAddLearnerStoreToNode{
			proposalBase: proposalBase{
				reply: p.makeReplyFunc(from, m, resp),
			},
			Msg: msg,
		})
	}
}

func (p *pd) onPromoteLearnerStore(from *net.UDPAddr, m *snet.Message) {
	msg := m.Msg.(*sproto.PromoteLearnerStore)
	resp := &sproto.PromoteLearnerStoreResp{}

	err := func() error {
		if !(msg.Store > 0 && msg.Store <= int32(StorePerSet)) {
			return errors.New(fmt.Sprintf("storeID must between(1,%d)", StorePerSet))
		}

		if nil == p.pState.deployment {
			return errors.New("must init deployment first")
		}

		s, ok := p.pState.deployment.sets[int(msg.SetID)]
		if !ok {
			return errors.New("set not found")
		}

		n, ok := s.nodes[int(msg.NodeID)]
		if !ok {
			return errors.New("node not found")
		}

		st, ok := n.store[int(msg.Store)]
		if ok {
			if st.Type == VoterStore {
				if st.Value == FlyKvUnCommit {
					return errors.New("voter wait for commit by flykv")
				} else {
					return errors.New("store is already a voter")
				}
			} else if st.Type == LearnerStore {
				if st.Value == FlyKvUnCommit {
					return errors.New("learner wait for commit by flykv")
				} else {
					return nil
				}
			} else {
				return errors.New("remove store wait for commit by flykv")
			}
		} else {
			return errors.New("store is not found")
		}
	}()

	if nil != err {
		resp.Ok = false
		resp.Reason = err.Error()
		p.udp.SendTo(from, snet.MakeMessage(m.Context, resp))
	} else {
		p.issueProposal(&ProposalPromoteLearnerStore{
			proposalBase: proposalBase{
				reply: p.makeReplyFunc(from, m, resp),
			},
			Msg: msg,
		})
	}
}

func (p *pd) onRemoveNodeStore(from *net.UDPAddr, m *snet.Message) {

	GetSugar().Debugf("onRemoveNodeStore")

	msg := m.Msg.(*sproto.RemoveNodeStore)
	resp := &sproto.RemoveNodeStoreResp{}

	s := p.pState.deployment.sets[int(msg.SetID)]
	n := s.nodes[int(msg.NodeID)]
	store := n.store[int(msg.Store)]

	err := func() error {
		if nil == store {
			return errors.New("store not exists")
		} else if store.Type == RemoveStore {
			return errors.New("store is removing")
		} else if store.Value == FlyKvUnCommit {
			return errors.New("wait for previous op commit by flykv")
		} else {
			return nil
		}
	}()

	if nil != err {
		resp.Ok = false
		resp.Reason = err.Error()
		p.udp.SendTo(from, snet.MakeMessage(m.Context, resp))
	} else {
		p.issueProposal(&ProposalRemoveNodeStore{
			proposalBase: proposalBase{
				reply: p.makeReplyFunc(from, m, resp),
			},
			Msg: msg,
		})
	}
}
