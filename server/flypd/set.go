package flypd

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/sniperHW/flyfish/pkg/bitmap"
	"github.com/sniperHW/flyfish/pkg/buffer"
	snet "github.com/sniperHW/flyfish/server/net"
	sproto "github.com/sniperHW/flyfish/server/proto"
	"github.com/sniperHW/flyfish/server/slot"
	"net"
	"sort"
)

type ProposalAddSet struct {
	*proposalBase
	msg *sproto.AddSet
}

func (p *ProposalAddSet) Serilize(b []byte) []byte {
	b = buffer.AppendByte(b, byte(proposalAddSet))
	bb, err := json.Marshal(p.msg)
	if nil != err {
		panic(err)
	}
	return buffer.AppendBytes(b, bb)
}

func (p *ProposalAddSet) doApply() error {

	if nil != p.pd.pState.MetaTransaction {
		return errors.New("wait for previous meta transaction finish")
	}

	if _, ok := p.pd.pState.deployment.sets[int(p.msg.Set.SetID)]; ok {
		return errors.New("set already exists")
	}

	s := &set{
		id:     int(p.msg.Set.SetID),
		nodes:  map[int]*kvnode{},
		stores: map[int]*store{},
	}

	for _, v := range p.msg.Set.Nodes {
		s.nodes[int(v.NodeID)] = &kvnode{
			id:          int(v.NodeID),
			host:        v.Host,
			servicePort: int(v.ServicePort),
			raftPort:    int(v.RaftPort),
			set:         s,
		}
	}

	var stores []int

	for _, v := range p.pd.pState.deployment.sets {
		for _, vv := range v.stores {
			stores = append(stores, vv.id)
		}
	}

	sort.Slice(stores, func(i, j int) bool {
		return stores[i] < stores[j]
	})

	var i int
	for i := 0; i < len(stores)-1; i++ {
		if stores[i]+1 != stores[i+1] {
			break
		}
	}

	var beg int
	if stores[i]+1 != stores[i+1] {
		beg = stores[i] + 1
		if beg+StorePerSet >= stores[i+1] {
			panic("error here")
		}
	} else {
		beg = stores[i+1] + 1
	}

	for i := 0; i < StorePerSet; i++ {
		st := &store{
			id:    beg + i,
			slots: bitmap.New(slot.SlotCount),
			set:   s,
		}
		s.stores[st.id] = st
	}

	p.pd.pState.deployment.version++
	s.version = p.pd.pState.deployment.version
	p.pd.pState.deployment.sets[s.id] = s

	return nil

}

func (p *ProposalAddSet) apply() {
	err := p.doApply()

	if nil == err {
		//添加新set的操作通过，开始执行slot平衡
		p.pd.slotBalance()
	}

	if nil != p.reply {
		p.reply(err)
	}

}

func (p *pd) replayAddSet(reader *buffer.BufferReader) error {
	var msg sproto.AddSet
	if err := json.Unmarshal(reader.GetAll(), &msg); nil != err {
		return err
	}

	pr := &ProposalAddSet{
		proposalBase: &proposalBase{
			pd: p,
		},
		msg: &msg,
	}
	pr.doApply()

	return nil
}

type ProposalRemSet struct {
	*proposalBase
	setID int
}

func (p *ProposalRemSet) Serilize(b []byte) []byte {
	b = buffer.AppendByte(b, byte(proposalRemSet))
	return buffer.AppendInt32(b, int32(p.setID))
}

func (p *ProposalRemSet) apply() {

	err := func() error {
		if nil != p.pd.pState.MetaTransaction {
			return errors.New("wait for previous meta transaction finish")
		}

		s, ok := p.pd.pState.deployment.sets[int(p.setID)]
		if !ok {
			return errors.New("set not exists")
		}

		//只有当s中所有的store都不存在slot时才能移除
		for _, v := range s.stores {
			if len(v.slots.GetOpenBits()) != 0 {
				return errors.New(fmt.Sprintf("there are slots in store:%d", v.id))
			}
		}

		//如果有slot要向set迁移，不允许删除
		for _, v := range p.pd.pState.SlotTransfer {
			if v.SetIn == s.id {
				return errors.New("there are slots trans in")
			}
		}

		return nil
	}()

	if nil == err {
		delete(p.pd.pState.deployment.sets, p.setID)
		p.pd.pState.deployment.version++
	}

	if nil != p.reply {
		p.reply(err)
	}

}

func (p *pd) replayRemSet(reader *buffer.BufferReader) error {
	pa := &ProposalRemSet{
		setID: int(reader.GetInt32()),
		proposalBase: &proposalBase{
			pd: p,
		},
	}
	pa.apply()
	return nil
}

type ProposalSetMarkClear struct {
	*proposalBase
	setID int
}

func (p *ProposalSetMarkClear) Serilize(b []byte) []byte {
	b = buffer.AppendByte(b, byte(proposalSetMarkClear))
	return buffer.AppendInt32(b, int32(p.setID))
}

func (p *ProposalSetMarkClear) doApply() error {
	var ok bool
	var s *set
	err := func() error {
		s, ok = p.pd.pState.deployment.sets[int(p.setID)]
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
		p.pd.markClearSet[p.setID] = s
	}

	return err

}

func (p *ProposalSetMarkClear) apply() {
	err := p.doApply()
	if nil == err {
		p.pd.slotBalance()
	}

	if nil != p.reply {
		p.reply(err)
	}

}

func (p *pd) replaySetMarkClear(reader *buffer.BufferReader) error {
	pa := &ProposalSetMarkClear{
		setID: int(reader.GetInt32()),
		proposalBase: &proposalBase{
			pd: p,
		},
	}
	pa.doApply()
	return nil
}

func (p *pd) onRemSet(from *net.UDPAddr, m *snet.Message) {
	msg := m.Msg.(*sproto.RemSet)

	resp := &sproto.RemSetResp{}

	err := func() error {
		if nil != p.pState.MetaTransaction {
			return errors.New("wait for previous meta transaction finish")
		}

		if nil == p.pState.deployment {
			return errors.New("no deployment")
		}

		s, ok := p.pState.deployment.sets[int(msg.SetID)]
		if !ok {
			return errors.New("set not exists")
		}

		//只有当s中所有的store都不存在slot时才能移除
		for _, v := range s.stores {
			if len(v.slots.GetOpenBits()) != 0 {
				return errors.New(fmt.Sprintf("there are slots in store:%d", v.id))
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
	} else {
		p.issueProposal(&ProposalRemSet{
			setID: int(msg.SetID),
			proposalBase: &proposalBase{
				pd:    p,
				reply: p.makeReplyFunc(from, m, resp),
			},
		})
	}
}

func (p *pd) onSetMarkClear(from *net.UDPAddr, m *snet.Message) {
	msg := m.Msg.(*sproto.SetMarkClear)

	resp := &sproto.SetMarkClearResp{}

	err := func() error {
		if nil == p.pState.deployment {
			return errors.New("no deployment")
		}

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
			setID: int(msg.SetID),
			proposalBase: &proposalBase{
				pd:    p,
				reply: p.makeReplyFunc(from, m, resp),
			},
		})
	} else {
		resp.Ok = false
		resp.Reason = err.Error()
		p.udp.SendTo(from, snet.MakeMessage(m.Context, resp))
	}

}

func (p *pd) onAddSet(from *net.UDPAddr, m *snet.Message) {
	msg := m.Msg.(*sproto.AddSet)
	resp := &sproto.AddSetResp{}

	err := func() error {
		if nil != p.pState.MetaTransaction {
			return errors.New("wait for previous meta transaction finish")
		}

		if nil == p.pState.deployment {
			return errors.New("no deployment")
		}

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
				return errors.New(fmt.Sprintf("duplicate node:%d", v.NodeID))
			}

			if nodeServices[fmt.Sprintf("%s:%d", v.Host, v.ServicePort)] {
				return errors.New(fmt.Sprintf("duplicate service:%s:%d", v.Host, v.ServicePort))
			}

			if nodeRafts[fmt.Sprintf("%s:%d", v.Host, v.RaftPort)] {
				return errors.New(fmt.Sprintf("duplicate inter:%s:%d", v.Host, v.RaftPort))
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
		p.udp.SendTo(from, snet.MakeMessage(m.Context, resp))
	} else {
		p.issueProposal(&ProposalAddSet{
			msg: msg,
			proposalBase: &proposalBase{
				pd:    p,
				reply: p.makeReplyFunc(from, m, resp),
			},
		})
	}

}
