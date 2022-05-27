package flypd

import (
	"errors"
	"github.com/sniperHW/flyfish/pkg/bitmap"
	snet "github.com/sniperHW/flyfish/server/net"
	sproto "github.com/sniperHW/flyfish/server/proto"
	"github.com/sniperHW/flyfish/server/slot"
)

type ProposalAddSet struct {
	proposalBase
	Set SetJson
}

func (p *ProposalAddSet) Serilize(b []byte) []byte {
	return serilizeProposal(b, proposalAddSet, p)
}

func (p *ProposalAddSet) apply(pd *pd) {
	GetSugar().Infof("onAddSet apply")
	err := func() error {
		deploymentJson := pd.pState.deployment.toDeploymentJson()
		deploymentJson.Version++
		p.Set.Version = deploymentJson.Version
		deploymentJson.Sets = append(deploymentJson.Sets, p.Set)
		if err := deploymentJson.check(); nil != err {
			return err
		} else {
			pd.pState.deployment.loadFromDeploymentJson(&deploymentJson)
			pd.slotBalance()
			return nil
		}
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
	s, ok := pd.pState.deployment.sets[int(p.SetID)]
	if !ok {
		err = errors.New("set not exists")
	} else {
		delete(pd.pState.deployment.sets, p.SetID)
		pd.pState.deployment.version++
		pd.pState.SlotTransferMgr.onSetRemove(pd, s)
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

		if len(pd.pState.deployment.sets) == 1 {
			return errors.New("can't mark clear the only set")
		}

		return nil
	}()

	if nil == err {
		s.markClear = true
		pd.slotBalance()
	}

	if nil != p.reply {
		p.reply(err)
	}
}

func (p *pd) onRemSet(replyer replyer, m *snet.Message) {
	msg := m.Msg.(*sproto.RemSet)

	resp := &sproto.RemSetResp{}

	err := func() error {
		_, ok := p.pState.deployment.sets[int(msg.SetID)]
		if !ok {
			return errors.New("set not exists")
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
	GetSugar().Infof("onAddSet")
	msg := m.Msg.(*sproto.AddSet)
	resp := &sproto.AddSetResp{}

	set := SetJson{
		SetID: int(msg.Set.SetID),
	}

	err := func() error {
		if _, ok := p.pState.deployment.sets[int(msg.Set.SetID)]; ok {
			return errors.New("set already exists")
		}

		deploymentJson := p.pState.deployment.toDeploymentJson()

		for _, v := range msg.Set.Nodes {
			set.KvNodes = append(set.KvNodes, KvNodeJson{
				NodeID:      int(v.NodeID),
				Host:        v.Host,
				ServicePort: int(v.ServicePort),
				RaftPort:    int(v.RaftPort),
				Store:       map[int]*FlyKvStoreState{},
			})
		}

		for i := 0; i < StorePerSet; i++ {
			set.Stores = append(set.Stores, StoreJson{
				StoreID: i + 1,
				Slots:   bitmap.New(slot.SlotCount).ToJson(),
			})
		}

		for i, _ := range set.KvNodes {
			for _, vv := range set.Stores {
				set.KvNodes[i].Store[vv.StoreID] = &FlyKvStoreState{
					Type:   VoterStore,
					Value:  FlyKvCommited,
					RaftID: p.RaftIDGen.Next(),
				}
			}
		}

		deploymentJson.Sets = append(deploymentJson.Sets, set)

		return deploymentJson.check()
	}()

	if nil != err {
		GetSugar().Infof("onAddSet %v %v", *msg, err)
		resp.Ok = false
		resp.Reason = err.Error()
		replyer.reply(snet.MakeMessage(m.Context, resp))
	} else {
		GetSugar().Infof("onAddSet %v %v", *msg, err)
		p.issueProposal(&ProposalAddSet{
			Set: set,
			proposalBase: proposalBase{
				reply: p.makeReplyFunc(replyer, m, resp),
			},
		})
	}
}
