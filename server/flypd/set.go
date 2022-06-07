package flypd

import (
	"errors"
	"fmt"
	"github.com/sniperHW/flyfish/pkg/bitmap"
	snet "github.com/sniperHW/flyfish/server/net"
	sproto "github.com/sniperHW/flyfish/server/proto"
	"github.com/sniperHW/flyfish/server/slot"
)

type ProposalAddSet struct {
	proposalBase
	Set *Set
}

func (p *ProposalAddSet) Serilize(b []byte) []byte {
	return serilizeProposal(b, proposalAddSet, p)
}

func (p *ProposalAddSet) apply(pd *pd) {
	GetSugar().Infof("onAddSet apply")
	err := func() error {
		if _, ok := pd.Deployment.Sets[p.Set.SetID]; ok {
			return fmt.Errorf("duplicate set %d", p.Set.SetID)
		}

		nodes := map[int]bool{}
		services := map[string]bool{}
		raftServices := map[string]bool{}

		for _, set := range pd.Deployment.Sets {
			for _, node := range set.Nodes {
				nodes[node.NodeID] = true
				services[fmt.Sprintf("%s:%d", node.Host, node.ServicePort)] = true
				raftServices[fmt.Sprintf("%s:%d", node.Host, node.RaftPort)] = true
			}
		}

		for _, v := range p.Set.Nodes {
			if _, ok := nodes[v.NodeID]; ok {
				return fmt.Errorf("duplicate node %d", v.NodeID)
			}

			if _, ok := services[fmt.Sprintf("%s:%d", v.Host, v.ServicePort)]; ok {
				return fmt.Errorf("duplicate service %s:%d", v.Host, v.ServicePort)
			}

			if _, ok := raftServices[fmt.Sprintf("%s:%d", v.Host, v.RaftPort)]; ok {
				return fmt.Errorf("duplicate raftService %s:%d", v.Host, v.RaftPort)
			}
		}

		for i := 0; i < StorePerSet; i++ {
			slots := bitmap.New(slot.SlotCount)
			p.Set.Stores[i+1] = &Store{
				StoreID: i + 1,
				Slots:   slots.ToJson(),
				slots:   slots,
			}
		}

		pd.Deployment.Sets[p.Set.SetID] = p.Set
		pd.Deployment.Version++
		pd.slotBalance()
		return nil
	}()

	GetSugar().Infof("onAddSet apply %v", err)

	if nil != p.reply {
		p.reply(err)
	}
}

type ProposalRemSet struct {
	proposalBase
	SetID int
}

func (p *ProposalRemSet) Serilize(b []byte) []byte {
	return serilizeProposal(b, proposalRemSet, p)
}

func (p *ProposalRemSet) apply(pd *pd) {
	var err error
	set, ok := pd.Deployment.Sets[int(p.SetID)]
	if !ok {
		err = errors.New("set not exists")
	} else {
		delete(pd.Deployment.Sets, p.SetID)
		pd.Deployment.Version++
		pd.SlotTransferMgr.onSetRemove(pd, set)
	}

	if nil != p.reply {
		p.reply(err)
	}
}

type ProposalSetMarkClear struct {
	proposalBase
	SetID int
}

func (p *ProposalSetMarkClear) Serilize(b []byte) []byte {
	return serilizeProposal(b, proposalSetMarkClear, p)
}

func (p *ProposalSetMarkClear) apply(pd *pd) {
	err := func() error {
		s, ok := pd.Deployment.Sets[int(p.SetID)]
		if !ok {
			return errors.New("set not exists")
		}

		if s.MarkClear {
			return errors.New("already mark clear")
		}

		if len(pd.Deployment.Sets) == 1 {
			return errors.New("can't mark clear the only set")
		}

		s.MarkClear = true
		pd.slotBalance()
		return nil

	}()

	if nil != p.reply {
		p.reply(err)
	}
}

func (p *pd) onRemSet(replyer replyer, m *snet.Message) {
	msg := m.Msg.(*sproto.RemSet)
	p.issueProposal(&ProposalRemSet{
		SetID: int(msg.SetID),
		proposalBase: proposalBase{
			reply: p.makeReplyFunc(replyer, m, &sproto.RemSetResp{}),
		},
	})
}

func (p *pd) onSetMarkClear(replyer replyer, m *snet.Message) {
	msg := m.Msg.(*sproto.SetMarkClear)
	p.issueProposal(&ProposalSetMarkClear{
		SetID: int(msg.SetID),
		proposalBase: proposalBase{
			reply: p.makeReplyFunc(replyer, m, &sproto.SetMarkClearResp{}),
		},
	})
}

func (p *pd) onAddSet(replyer replyer, m *snet.Message) {
	GetSugar().Infof("onAddSet")
	msg := m.Msg.(*sproto.AddSet)

	set := Set{
		SetID:  int(msg.Set.SetID),
		Nodes:  map[int]*KvNode{},
		Stores: map[int]*Store{},
	}

	for _, v := range msg.Set.Nodes {
		set.Nodes[int(v.NodeID)] = &KvNode{
			NodeID:      int(v.NodeID),
			Host:        v.Host,
			ServicePort: int(v.ServicePort),
			RaftPort:    int(v.RaftPort),
			Store:       map[int]*NodeStoreState{},
		}
	}

	for i, _ := range set.Nodes {
		for _, vv := range set.Stores {
			set.Nodes[i].Store[vv.StoreID] = &NodeStoreState{
				StoreType: VoterStore,
				RaftID:    p.raftIDGen.Next(),
			}
		}
	}

	p.issueProposal(&ProposalAddSet{
		Set: &set,
		proposalBase: proposalBase{
			reply: p.makeReplyFunc(replyer, m, &sproto.AddSetResp{}),
		},
	})
}
