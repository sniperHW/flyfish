package flypd

import (
	"encoding/json"
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

func (p *ProposalAddSet) doApply() {

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

}

func (p *ProposalAddSet) apply() {
	p.doApply()
	p.reply(nil)
	//添加新set的操作通过，开始执行slot平衡
	p.pd.slotBalance()
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
	delete(p.pd.pState.deployment.sets, p.setID)
	p.pd.pState.deployment.version++
	p.reply(nil)
}

func (p *pd) replayRemSet(reader *buffer.BufferReader) error {
	setID := int(reader.GetInt32())
	delete(p.pState.deployment.sets, setID)
	p.pState.deployment.version++
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

func (p *ProposalSetMarkClear) apply() {
	if set, ok := p.pd.pState.deployment.sets[p.setID]; ok && !set.markClear {
		set.markClear = true
		p.pd.pState.markClearSet[p.setID] = set
		p.pd.slotBalance()
	}
	p.reply(nil)
}

func (p *pd) replaySetMarkClear(reader *buffer.BufferReader) error {
	setID := int(reader.GetInt32())
	if set, ok := p.pState.deployment.sets[setID]; ok && !set.markClear {
		set.markClear = true
		p.pState.markClearSet[setID] = set
	}
	return nil
}

func (p *pd) onRemSet(from *net.UDPAddr, m *snet.Message) {
	msg := m.Msg.(*sproto.RemSet)

	resp := &sproto.RemSetResp{}

	if nil != p.pState.MetaTransaction {
		resp.Ok = false
		resp.Reason = "wait for previous meta transaction finish"
		p.udp.SendTo(from, snet.MakeMessage(m.Context, resp))
		return
	}

	if nil == p.pState.deployment {
		resp.Ok = false
		resp.Reason = "no deployment"
		p.udp.SendTo(from, snet.MakeMessage(m.Context, resp))
		return
	}

	s, ok := p.pState.deployment.sets[int(msg.SetID)]
	if !ok {
		resp.Ok = false
		resp.Reason = "set not exists"
		p.udp.SendTo(from, snet.MakeMessage(m.Context, resp))
		return
	}

	//只有当s中所有的store都不存在slot时才能移除
	for _, v := range s.stores {
		if len(v.slots.GetOpenBits()) != 0 {
			resp.Ok = false
			resp.Reason = fmt.Sprintf("there are slots in store:%d", v.id)
			p.udp.SendTo(from, snet.MakeMessage(m.Context, resp))
			return
		}
	}

	p.issueProposal(&ProposalRemSet{
		setID: int(msg.SetID),
		proposalBase: &proposalBase{
			pd:    p,
			reply: p.makeReplyFunc(from, m, resp),
		},
	})
}

func (p *pd) onSetMarkClear(from *net.UDPAddr, m *snet.Message) {
	msg := m.Msg.(*sproto.SetMarkClear)

	resp := &sproto.SetMarkClearResp{}

	if nil == p.pState.deployment {
		resp.Ok = false
		resp.Reason = "no deployment"
		p.udp.SendTo(from, snet.MakeMessage(m.Context, resp))
		return
	}

	s, ok := p.pState.deployment.sets[int(msg.SetID)]
	if !ok {
		resp.Ok = false
		resp.Reason = "set not exists"
		p.udp.SendTo(from, snet.MakeMessage(m.Context, resp))
		return
	}

	if s.markClear {
		resp.Ok = true
		p.udp.SendTo(from, snet.MakeMessage(m.Context, resp))
		return
	}

	p.issueProposal(&ProposalSetMarkClear{
		setID: int(msg.SetID),
		proposalBase: &proposalBase{
			pd:    p,
			reply: p.makeReplyFunc(from, m, resp),
		},
	})

}

func (p *pd) onAddSet(from *net.UDPAddr, m *snet.Message) {
	msg := m.Msg.(*sproto.AddSet)

	resp := &sproto.AddSetResp{}

	if nil != p.pState.MetaTransaction {
		resp.Ok = false
		resp.Reason = "wait for previous meta transaction finish"
		p.udp.SendTo(from, snet.MakeMessage(m.Context, resp))
		return
	}

	if nil == p.pState.deployment {
		resp.Ok = false
		resp.Reason = "no deployment"
		p.udp.SendTo(from, snet.MakeMessage(m.Context, resp))
		return
	}

	_, ok := p.pState.deployment.sets[int(msg.Set.SetID)]
	if ok {
		resp.Ok = false
		resp.Reason = "set already exists"
		p.udp.SendTo(from, snet.MakeMessage(m.Context, resp))
		return
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
			resp.Ok = false
			resp.Reason = fmt.Sprintf("duplicate node:%d", v.NodeID)
			p.udp.SendTo(from, snet.MakeMessage(m.Context, resp))
			return
		}

		if nodeServices[fmt.Sprintf("%s:%d", v.Host, v.ServicePort)] {
			resp.Ok = false
			resp.Reason = fmt.Sprintf("duplicate service:%s:%d", v.Host, v.ServicePort)
			p.udp.SendTo(from, snet.MakeMessage(m.Context, resp))
			return
		}

		if nodeRafts[fmt.Sprintf("%s:%d", v.Host, v.RaftPort)] {
			resp.Ok = false
			resp.Reason = fmt.Sprintf("duplicate inter:%s:%d", v.Host, v.RaftPort)
			p.udp.SendTo(from, snet.MakeMessage(m.Context, resp))
			return
		}

		nodeIDS[int(v.NodeID)] = true
		nodeServices[fmt.Sprintf("%s:%d", v.Host, v.ServicePort)] = true
		nodeRafts[fmt.Sprintf("%s:%d", v.Host, v.RaftPort)] = true
	}

	p.issueProposal(&ProposalAddSet{
		msg: msg,
		proposalBase: &proposalBase{
			pd:    p,
			reply: p.makeReplyFunc(from, m, resp),
		},
	})

}
