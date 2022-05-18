package flypd

import (
	"errors"
	"fmt"
	"github.com/sniperHW/flyfish/pkg/raft/membership"
	snet "github.com/sniperHW/flyfish/server/net"
	sproto "github.com/sniperHW/flyfish/server/proto"
	"sync"
	"time"
)

type storeTask struct {
	mtx            sync.Mutex //protect timer
	node           *kvnode
	store          int
	raftID         uint64
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
				RaftID: st.raftID,
			}

			if st.storeStateType == LearnerStore {
				msg.Host = st.node.host
				msg.RaftPort = int32(st.node.raftPort)
				msg.Port = int32(st.node.servicePort)
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
					st.mtx.Lock()
					st.timer = time.AfterFunc(time.Second*3, func() {
						st.pd.mainque.AppendHighestPriotiryItem(st.notifyFlyKv)
					})
					st.mtx.Unlock()
				}
			}()
		}
	}
}

func (p *pd) startStoreNotifyTask(node *kvnode, store int, raftID uint64, storeStateType FlyKvStoreStateType) {

	GetSugar().Infof("startStoreNotifyTask %d %d %d", node.id, store, raftID)

	taskID := uint64(node.id)<<32 + uint64(store)
	t, ok := p.storeTask[taskID]
	if !ok {
		t = &storeTask{
			node:           node,
			pd:             p,
			store:          store,
			storeStateType: storeStateType,
			raftID:         raftID,
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

	GetSugar().Infof("ProposalFlyKvCommited apply type:%v set:%d node:%d store:%d", p.Type, p.Set, p.Node, p.Store)

	s := pd.pState.deployment.sets[p.Set]
	n := s.nodes[p.Node]

	st := n.store[p.Store]

	switch p.Type {
	case LearnerStore, VoterStore:
		st.Value = FlyKvCommited
		if p.Type == LearnerStore {
			st.Type = VoterStore
			st.Value = FlyKvUnCommit
		}
	case RemoveStore:
		delete(n.store, p.Store)
	}

	taskID := uint64(p.Node)<<32 + uint64(p.Store)

	_, ok := pd.storeTask[taskID]
	if ok {
		delete(pd.storeTask, taskID)
	}

	if n.removing && len(n.store) == 0 {
		GetSugar().Infof("ProposalFlyKvCommited apply remove set:%d node:%d", p.Set, p.Node)
		delete(pd.pendingNodes, n.id)
		delete(s.nodes, n.id)
		pd.pState.deployment.version++
		s.version = pd.pState.deployment.version
	} else if n.storeIsOk() {
		delete(pd.pendingNodes, n.id)
		GetSugar().Infof("ProposalFlyKvCommited apply ok set:%d node:%d", p.Set, p.Node)
	}
}

type ProposalAddNode struct {
	proposalBase
	Msg     *sproto.AddNode
	RaftIDS []uint64
}

func (p *ProposalAddNode) Serilize(b []byte) []byte {
	return serilizeProposal(b, proposalAddNode, p)
}

func (p *ProposalAddNode) apply(pd *pd) {
	var s *set
	var ok bool

	GetSugar().Infof("onAddNode.apply")

	err := func() error {
		s, ok = pd.pState.deployment.sets[int(p.Msg.SetID)]
		if !ok {
			return errors.New("set not found")
		}

		if v, ok := pd.pendingNodes[int(p.Msg.NodeID)]; ok {
			if v.removing {
				return errors.New("node is removing")
			}

			if !v.storeIsOk() {
				return errors.New("node is adding")
			}
		}

		for _, v := range pd.pState.deployment.sets {
			for _, vv := range v.nodes {
				if vv.id == int(p.Msg.NodeID) {
					return errors.New("duplicate node id")
				}

				if vv.host == p.Msg.Host && vv.servicePort == int(p.Msg.ServicePort) {
					return errors.New("duplicate service addr")
				}

				if vv.host == p.Msg.Host && vv.raftPort == int(p.Msg.RaftPort) {
					return errors.New("duplicate raft addr")
				}
			}
		}

		for i := 0; i < StorePerSet; i++ {

			learnerNum := 0

			for _, v := range s.nodes {
				if v.isLearner(i + 1) {
					learnerNum++
				}
			}

			//不允许超过同时存在的learner数量限制
			if learnerNum+1 > membership.MaxLearners {
				return errors.New("too many learner")
			}
		}

		return nil
	}()

	if nil == err {
		n := &kvnode{
			id:          int(p.Msg.NodeID),
			host:        p.Msg.Host,
			servicePort: int(p.Msg.ServicePort),
			raftPort:    int(p.Msg.RaftPort),
			set:         s,
			store:       map[int]*FlyKvStoreState{},
		}

		for i := 0; i < StorePerSet; i++ {
			n.store[i+1] = &FlyKvStoreState{
				Type:   LearnerStore,
				Value:  FlyKvUnCommit,
				RaftID: p.RaftIDS[i],
			}

			if pd.isLeader() {
				pd.startStoreNotifyTask(n, i+1, p.RaftIDS[i], LearnerStore)
			}
		}

		s.nodes[int(p.Msg.NodeID)] = n
		pd.pState.deployment.version++
		s.version = pd.pState.deployment.version
		pd.pendingNodes[n.id] = n
	}

	if nil != p.reply {
		p.reply(err)
	}
}

func (p *pd) onAddNode(replyer replyer, m *snet.Message) {

	GetSugar().Infof("onAddNode")

	msg := m.Msg.(*sproto.AddNode)
	resp := &sproto.AddNodeResp{}

	err := func() error {
		s, ok := p.pState.deployment.sets[int(msg.SetID)]
		if !ok {
			return errors.New("set not found")
		}

		if v, ok := p.pendingNodes[int(msg.NodeID)]; ok {
			if v.removing {
				return errors.New("node is removing")
			}

			if !v.storeIsOk() {
				return errors.New("node is adding")
			}
		}

		for _, v := range p.pState.deployment.sets {
			for _, vv := range v.nodes {
				if vv.id == int(msg.NodeID) {
					return errors.New("duplicate node id")
				}

				if vv.host == msg.Host && vv.servicePort == int(msg.ServicePort) {
					return errors.New("duplicate service addr")
				}

				if vv.host == msg.Host && vv.raftPort == int(msg.RaftPort) {
					return errors.New("duplicate raft addr")
				}
			}
		}

		for i := 0; i < StorePerSet; i++ {

			learnerNum := 0

			for _, v := range s.nodes {
				if v.isLearner(i + 1) {
					learnerNum++
				}
			}

			//不允许超过同时存在的learner数量限制
			if learnerNum+1 > membership.MaxLearners {
				return errors.New("too many learner")
			}
		}

		return nil
	}()

	if nil != err {
		resp.Ok = false
		resp.Reason = err.Error()
		replyer.reply(snet.MakeMessage(m.Context, resp))
	} else {

		pro := &ProposalAddNode{
			Msg: msg,
			proposalBase: proposalBase{
				reply: p.makeReplyFunc(replyer, m, resp),
			},
		}

		for i := 0; i < StorePerSet; i++ {
			pro.RaftIDS = append(pro.RaftIDS, p.RaftIDGen.Next())
		}

		p.issueProposal(pro)
	}
}

type ProposalRemNode struct {
	proposalBase
	Msg *sproto.RemNode
}

func (p *ProposalRemNode) Serilize(b []byte) []byte {
	return serilizeProposal(b, proposalRemNode, p)
}

func (p *ProposalRemNode) apply(pd *pd) {
	var s *set
	var ok bool
	var n *kvnode

	err := func() error {

		s, ok = pd.pState.deployment.sets[int(p.Msg.SetID)]
		if !ok {
			return errors.New("set not found")
		}

		n, ok = s.nodes[int(p.Msg.NodeID)]
		if !ok {
			return errors.New("node not found")
		}

		if v, ok := pd.pendingNodes[int(p.Msg.NodeID)]; ok {
			if v.removing {
				return errors.New("node is removing")
			} else {
				return errors.New("node is adding")
			}
		}

		if len(s.nodes) == 1 {
			return errors.New("can't remove the last node")
		}

		return nil
	}()

	if nil == err {
		n.removing = true
		if pd.isLeader() {
			for k, v := range n.store {
				v.Type = RemoveStore
				v.Value = FlyKvUnCommit
				pd.startStoreNotifyTask(n, k, v.RaftID, RemoveStore)
			}
		}
		GetSugar().Infof("ProposalRemNode.apply set:%d,remnode:%d nodecount:%d", p.Msg.SetID, p.Msg.NodeID, len(s.nodes))
	} else {
		GetSugar().Infof("ProposalRemNode.apply error:%v", err)
	}

	if nil != p.reply {
		p.reply(err)
	}
}

func (p *pd) onRemNode(replyer replyer, m *snet.Message) {
	msg := m.Msg.(*sproto.RemNode)
	resp := &sproto.RemNodeResp{}

	err := func() error {
		s, ok := p.pState.deployment.sets[int(msg.SetID)]
		if !ok {
			return errors.New("set not found")
		}

		_, ok = s.nodes[int(msg.NodeID)]
		if !ok {
			return errors.New("node not found")
		}

		if v, ok := p.pendingNodes[int(msg.NodeID)]; ok {
			if v.removing {
				return errors.New("node is removing")
			} else {
				return errors.New("node is adding")
			}
		}

		if len(s.nodes) == 1 {
			return errors.New("can't remove the last node")
		}

		return nil
	}()

	if nil != err {
		resp.Ok = false
		resp.Reason = err.Error()
		replyer.reply(snet.MakeMessage(m.Context, resp))
	} else {
		p.issueProposal(&ProposalRemNode{
			Msg: msg,
			proposalBase: proposalBase{
				reply: p.makeReplyFunc(replyer, m, resp),
			},
		})
	}
}
