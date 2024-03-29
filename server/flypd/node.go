package flypd

import (
	"errors"
	"fmt"
	"github.com/sniperHW/flyfish/pkg/raft/membership"
	snet "github.com/sniperHW/flyfish/server/net"
	sproto "github.com/sniperHW/flyfish/server/proto"
	"sync/atomic"
	"time"
)

type ProposalFlyKvCommited struct {
	proposalBase
	Set   int
	Node  int
	Store int
	Op    int32
	//StoreType StoreType
}

func (p *ProposalFlyKvCommited) Serilize(b []byte) []byte {
	return serilizeProposal(b, proposalFlyKvCommited, p)
}

func (p *ProposalFlyKvCommited) apply(pd *pd) {
	set := pd.Deployment.Sets[p.Set]
	if nil == set {
		return
	}

	node := set.Nodes[p.Node]
	if nil == node {
		return
	}

	store := node.Store[p.Store]

	if nil == store {
		return
	}

	switch sproto.StoreOpType(p.Op) {
	case sproto.StoreOpType_AddLearner:
		store.Joinning = false
	case sproto.StoreOpType_PromoteLearner:
		store.StoreType = Voter
	case sproto.StoreOpType_UpdateURL:
		store.UpdateURL = false
	case sproto.StoreOpType_RemoveStore:
		delete(node.Store, p.Store)
		if len(node.Store) == 0 {
			delete(set.Nodes, p.Node)
		}
		pd.Deployment.Version++
		set.Version = pd.Deployment.Version
	}
}

type ProposalUpdateNodeURL struct {
	proposalBase
	Msg *sproto.UpdateNodeURL
}

func (p *ProposalUpdateNodeURL) Serilize(b []byte) []byte {
	return serilizeProposal(b, proposalUpdateNodeURL, p)
}

func (p *ProposalUpdateNodeURL) apply(pd *pd) {
	var set *Set
	var node *KvNode

	err := func() error {
		if set = pd.Deployment.Sets[int(p.Msg.SetID)]; nil == set {
			return errors.New("set not found")
		}

		if node = set.Nodes[int(p.Msg.NodeID)]; nil == node {
			return errors.New("node not found")
		}

		for _, v := range pd.Deployment.Sets {
			for _, vv := range v.Nodes {
				if vv.Host == p.Msg.Host && vv.ServicePort == int(p.Msg.ServicePort) {
					return errors.New("duplicate service addr")
				}

				if vv.Host == p.Msg.Host && vv.RaftPort == int(p.Msg.RaftPort) {
					return errors.New("duplicate raft addr")
				}
			}
		}

		return nil
	}()

	GetSugar().Infof("ProposalUpdateNodeURL apply %v", err)

	if nil == err {
		if p.Msg.Host != node.Host || node.ServicePort != int(p.Msg.ServicePort) || node.RaftPort != int(p.Msg.RaftPort) {
			node.Host = p.Msg.Host
			node.ServicePort = int(p.Msg.ServicePort)
			node.RaftPort = int(p.Msg.RaftPort)
			for i := 0; i < StorePerSet; i++ {
				node.Store[i+1].UpdateURL = true
			}
			pd.Deployment.Version++
			set.Version = pd.Deployment.Version
		}
	}

	if nil != p.reply {
		p.reply(err)
	}
}

func (p *pd) onUpdateURL(replyer replyer, m *snet.Message) {
	GetSugar().Infof("onUpdateURL")
	proposal := &ProposalUpdateNodeURL{
		Msg: m.Msg.(*sproto.UpdateNodeURL),
		proposalBase: proposalBase{
			reply: p.makeReplyFunc(replyer, m, &sproto.UpdateNodeURLResp{}),
		},
	}
	p.issueProposal(proposal)
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
	var set *Set

	err := func() error {
		if set = pd.Deployment.Sets[int(p.Msg.SetID)]; nil == set {
			return errors.New("set not found")
		}

		for _, v := range pd.Deployment.Sets {
			for _, vv := range v.Nodes {
				if vv.NodeID == int(p.Msg.NodeID) {
					return errors.New("duplicate node id")
				}

				if vv.Host == p.Msg.Host && vv.ServicePort == int(p.Msg.ServicePort) {
					return errors.New("duplicate service addr")
				}

				if vv.Host == p.Msg.Host && vv.RaftPort == int(p.Msg.RaftPort) {
					return errors.New("duplicate raft addr")
				}
			}
		}

		for i := 0; i < StorePerSet; i++ {

			learnerNum := 0

			for _, v := range set.Nodes {
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
		n := &KvNode{
			NodeID:      int(p.Msg.NodeID),
			Host:        p.Msg.Host,
			ServicePort: int(p.Msg.ServicePort),
			RaftPort:    int(p.Msg.RaftPort),
			Store:       map[int]*NodeStoreState{},
			Join:        true,
		}

		for i := 0; i < StorePerSet; i++ {
			n.Store[i+1] = &NodeStoreState{
				StoreType: Learner,
				RaftID:    p.RaftIDS[i],
				Joinning:  true,
			}
		}
		set.Nodes[int(p.Msg.NodeID)] = n
		pd.Deployment.Version++
		set.Version = pd.Deployment.Version
	}

	if nil != p.reply {
		p.reply(err)
	}
}

func (p *pd) onAddNode(replyer replyer, m *snet.Message) {
	proposal := &ProposalAddNode{
		Msg: m.Msg.(*sproto.AddNode),
		proposalBase: proposalBase{
			reply: p.makeReplyFunc(replyer, m, &sproto.AddNodeResp{}),
		},
	}

	for i := 0; i < StorePerSet; i++ {
		proposal.RaftIDS = append(proposal.RaftIDS, p.raftIDGen.Next())
	}

	p.issueProposal(proposal)
}

type ProposalRemNode struct {
	proposalBase
	Msg *sproto.RemNode
}

func (p *ProposalRemNode) Serilize(b []byte) []byte {
	return serilizeProposal(b, proposalRemNode, p)
}

func (p *ProposalRemNode) apply(pd *pd) {
	var node *KvNode
	err := func() error {
		set := pd.Deployment.Sets[int(p.Msg.SetID)]
		if nil == set {
			return errors.New("set not found")
		}

		if node = set.Nodes[int(p.Msg.NodeID)]; nil == node {
			return errors.New("node not found")
		}

		for _, v := range node.Store {
			if v.Removing {
				return errors.New("node is removing")
			}
		}

		if len(set.Nodes)-1 < MinReplicaPerSet {
			return errors.New("can't remove node")
		}

		return nil
	}()

	if nil == err {
		for _, v := range node.Store {
			v.Removing = true
		}
	}

	if nil != p.reply {
		p.reply(err)
	}
}

func (p *pd) onRemNode(replyer replyer, m *snet.Message) {
	p.issueProposal(&ProposalRemNode{
		Msg: m.Msg.(*sproto.RemNode),
		proposalBase: proposalBase{
			reply: p.makeReplyFunc(replyer, m, &sproto.RemNodeResp{}),
		},
	})
}

func (p *pd) processNodeNotify() {
	if p.isLeader() {
		for _, set := range p.Deployment.Sets {
			for _, node := range set.Nodes {
				for storeID, store := range node.Store {
					if (store.UpdateURL || store.Removing || store.StoreType == Learner) && atomic.CompareAndSwapInt32(&store.notifying, 0, 1) {
						msg := &sproto.NotifyNodeStoreOp{
							NodeID: int32(node.NodeID),
							Store:  int32(storeID),
							RaftID: store.RaftID,
						}

						if store.Joinning {
							msg.Host = node.Host
							msg.RaftPort = int32(node.RaftPort)
							msg.Port = int32(node.ServicePort)
							msg.Op = int32(sproto.StoreOpType_AddLearner)
						} else if store.UpdateURL {
							msg.Host = node.Host
							msg.RaftPort = int32(node.RaftPort)
							msg.Port = int32(node.ServicePort)
							msg.Op = int32(sproto.StoreOpType_UpdateURL)
						} else if store.StoreType == Learner {
							msg.Op = int32(sproto.StoreOpType_PromoteLearner)
						} else {
							msg.Op = int32(sproto.StoreOpType_RemoveStore)
						}

						var remotes []string

						for _, v := range set.Nodes {
							if _, ok := v.Store[storeID]; ok {
								remotes = append(remotes, fmt.Sprintf("%s:%d", v.Host, v.ServicePort))
							}
						}

						go func(setID int, store *NodeStoreState) {
							resp, _ := snet.UdpCall(remotes, msg, &sproto.NodeStoreOpOk{}, time.Second*3)
							if nil != resp {
								p.issueProposal(&ProposalFlyKvCommited{
									Set:   setID,
									Node:  int(msg.NodeID),
									Store: int(msg.Store),
									Op:    msg.Op,
								})
							}

							atomic.StoreInt32(&store.notifying, 0)
						}(set.SetID, store)
					}
				}
			}
		}
	}

	time.AfterFunc(time.Second, func() {
		p.mainque.AppendHighestPriotiryItem(p.processNodeNotify)
	})
}
