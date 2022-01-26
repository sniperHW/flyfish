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
	Msg *sproto.AddSet
}

func (p *ProposalAddSet) Serilize(b []byte) []byte {
	return serilizeProposal(b, proposalAddSet, p)
}

func (p *ProposalAddSet) doApply(pd *pd) error {
	if _, ok := pd.pState.deployment.sets[int(p.Msg.Set.SetID)]; ok {
		return errors.New("set already exists")
	}

	s := &set{
		id:     int(p.Msg.Set.SetID),
		nodes:  map[int]*kvnode{},
		stores: map[int]*store{},
	}

	for _, v := range p.Msg.Set.Nodes {
		s.nodes[int(v.NodeID)] = &kvnode{
			id:          int(v.NodeID),
			host:        v.Host,
			servicePort: int(v.ServicePort),
			raftPort:    int(v.RaftPort),
			set:         s,
			store:       map[int]*FlyKvStoreState{},
		}
	}

	for i := 0; i < StorePerSet; i++ {
		st := &store{
			id:    i + 1,
			slots: bitmap.New(slot.SlotCount),
			set:   s,
		}
		s.stores[st.id] = st
	}

	for _, v := range s.nodes {
		for _, vv := range s.stores {
			v.store[vv.id] = &FlyKvStoreState{
				Type:       VoterStore,
				Value:      FlyKvCommited,
				InstanceID: pd.pState.deployment.nextInstanceID(),
			}
		}
	}

	pd.pState.deployment.version++
	s.version = pd.pState.deployment.version
	pd.pState.deployment.sets[s.id] = s

	GetSugar().Debugf("ProposalAddSet apply %v", s)

	return nil

}

func (p *ProposalAddSet) apply(pd *pd) {
	err := p.doApply(pd)

	if nil == err {
		//添加新set的操作通过，开始执行slot平衡
		pd.slotBalance()
	}

	if nil != p.reply {
		p.reply(err)
	}

}

func (p *ProposalAddSet) replay(pd *pd) {
	p.doApply(pd)
}

type ProposalRemSet struct {
	proposalBase
	SetID int
}

func (p *ProposalRemSet) Serilize(b []byte) []byte {
	return serilizeProposal(b, proposalRemSet, p)
}

func (p *ProposalRemSet) apply(pd *pd) {

	err := func() error {
		s, ok := pd.pState.deployment.sets[int(p.SetID)]
		if !ok {
			return errors.New("set not exists")
		}

		//只有当s中所有的store都不存在slot时才能移除
		for _, v := range s.stores {
			if len(v.slots.GetOpenBits()) != 0 {
				return fmt.Errorf("there are slots in store:%d", v.id)
			}
		}

		//如果有slot要向set迁移，不允许删除
		for _, v := range pd.pState.SlotTransfer {
			if v.SetIn == s.id {
				return errors.New("there are slots trans in")
			}
		}

		return nil
	}()

	if nil == err {
		delete(pd.pState.deployment.sets, p.SetID)
		delete(pd.markClearSet, p.SetID)
		pd.pState.deployment.version++
	}

	if nil != p.reply {
		p.reply(err)
	}

}

func (p *ProposalRemSet) replay(pd *pd) {
	p.apply(pd)
}

type ProposalSetMarkClear struct {
	proposalBase
	SetID int
}

func (p *ProposalSetMarkClear) Serilize(b []byte) []byte {
	return serilizeProposal(b, proposalSetMarkClear, p)
}

func (p *ProposalSetMarkClear) doApply(pd *pd) error {
	var ok bool
	var s *set
	err := func() error {
		s, ok = pd.pState.deployment.sets[int(p.SetID)]
		if !ok {
			return errors.New("set not exists")
		}

		if s.markClear {
			return errors.New("already mark clear")
		}

		return nil
	}()

	if nil == err {
		s.markClear = true
		pd.markClearSet[p.SetID] = s
		//检查是否有待迁入但!ready的TransSlotTransfer,有的话将其删除
		for k, v := range pd.pState.SlotTransfer {
			if v.SetIn == s.id && !v.ready {
				delete(pd.pState.SlotTransfer, k)
			}
		}
	}

	return err

}

func (p *ProposalSetMarkClear) apply(pd *pd) {
	err := p.doApply(pd)
	if nil == err {
		pd.slotBalance()
	}

	if nil != p.reply {
		p.reply(err)
	}

}

func (p *ProposalSetMarkClear) replay(pd *pd) {
	p.doApply(pd)
}

func (p *pd) onRemSet(replyer replyer, m *snet.Message) {
	msg := m.Msg.(*sproto.RemSet)

	resp := &sproto.RemSetResp{}

	err := func() error {
		s, ok := p.pState.deployment.sets[int(msg.SetID)]
		if !ok {
			return errors.New("set not exists")
		}

		//只有当s中所有的store都不存在slot时才能移除
		for _, v := range s.stores {
			if len(v.slots.GetOpenBits()) != 0 {
				return fmt.Errorf("there are slots in store:%d", v.id)
			}
		}

		//如果有slot要向set迁移，不允许删除
		for _, v := range p.pState.SlotTransfer {
			if v.SetIn == s.id {
				return errors.New("there are slots trans in")
			}
		}

		return nil
	}()

	if nil != err {
		resp.Ok = false
		resp.Reason = err.Error()
		replyer.reply(snet.MakeMessage(m.Context, resp))
	} else {
		p.issueProposal(&ProposalRemSet{
			SetID: int(msg.SetID),
			proposalBase: proposalBase{
				reply: p.makeReplyFunc(replyer, m, resp),
			},
		})
	}
}

func (p *pd) onSetMarkClear(replyer replyer, m *snet.Message) {
	msg := m.Msg.(*sproto.SetMarkClear)

	resp := &sproto.SetMarkClearResp{}

	err := func() error {
		s, ok := p.pState.deployment.sets[int(msg.SetID)]
		if !ok {
			return errors.New("set not exists")
		}

		if s.markClear {
			return errors.New("already mark clear")
		}

		return nil
	}()

	if nil == err {
		p.issueProposal(&ProposalSetMarkClear{
			SetID: int(msg.SetID),
			proposalBase: proposalBase{
				reply: p.makeReplyFunc(replyer, m, resp),
			},
		})
	} else {
		resp.Ok = false
		resp.Reason = err.Error()
		replyer.reply(snet.MakeMessage(m.Context, resp))
	}
}

func (p *pd) onAddSet(replyer replyer, m *snet.Message) {
	msg := m.Msg.(*sproto.AddSet)
	resp := &sproto.AddSetResp{}

	err := func() error {
		if _, ok := p.pState.deployment.sets[int(msg.Set.SetID)]; ok {
			return errors.New("set already exists")
		}

		//检查node是否有冲突
		nodeIDS := map[int]bool{}
		nodeServices := map[string]bool{}
		nodeRafts := map[string]bool{}

		for _, v := range p.pState.deployment.sets {
			for _, vv := range v.nodes {
				nodeIDS[vv.id] = true
				nodeServices[fmt.Sprintf("%s:%d", vv.host, vv.servicePort)] = true
				nodeRafts[fmt.Sprintf("%s:%d", vv.host, vv.raftPort)] = true
			}
		}

		for _, v := range msg.Set.Nodes {
			if nodeIDS[int(v.NodeID)] {
				return fmt.Errorf("duplicate node:%d", v.NodeID)
			}

			if nodeServices[fmt.Sprintf("%s:%d", v.Host, v.ServicePort)] {
				return fmt.Errorf("duplicate service:%s:%d", v.Host, v.ServicePort)
			}

			if nodeRafts[fmt.Sprintf("%s:%d", v.Host, v.RaftPort)] {
				return fmt.Errorf("duplicate inter:%s:%d", v.Host, v.RaftPort)
			}

			nodeIDS[int(v.NodeID)] = true
			nodeServices[fmt.Sprintf("%s:%d", v.Host, v.ServicePort)] = true
			nodeRafts[fmt.Sprintf("%s:%d", v.Host, v.RaftPort)] = true
		}

		return nil
	}()

	if nil != err {
		resp.Ok = false
		resp.Reason = err.Error()
		replyer.reply(snet.MakeMessage(m.Context, resp))
	} else {
		GetSugar().Debugf("onAddSet %v", *msg)
		p.issueProposal(&ProposalAddSet{
			Msg: msg,
			proposalBase: proposalBase{
				reply: p.makeReplyFunc(replyer, m, resp),
			},
		})
	}
}
