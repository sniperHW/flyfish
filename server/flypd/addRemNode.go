package flypd

import (
	//"encoding/json"
	"errors"
	//"github.com/sniperHW/flyfish/pkg/buffer"
	snet "github.com/sniperHW/flyfish/server/net"
	sproto "github.com/sniperHW/flyfish/server/proto"
	"net"
)

type ProposalAddNode struct {
	proposalBase
	Msg *sproto.AddNode
}

func (p *ProposalAddNode) Serilize(b []byte) []byte {
	return serilizeProposal(b, proposalAddNode, p)
}

func (p *ProposalAddNode) apply(pd *pd) {
	var s *set
	var ok bool

	err := func() error {
		s, ok = pd.pState.deployment.sets[int(p.Msg.SetID)]
		if !ok {
			return errors.New("set not found")
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
		s.nodes[int(p.Msg.NodeID)] = n
		pd.pState.deployment.version++
	}

	if nil != p.reply {
		p.reply(err)
	}
}

func (p *ProposalAddNode) replay(pd *pd) {
	p.apply(pd)
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

	err := func() error {

		s, ok = pd.pState.deployment.sets[int(p.Msg.SetID)]
		if !ok {
			return errors.New("set not found")
		}

		n, ok := s.nodes[int(p.Msg.NodeID)]
		if !ok {
			return errors.New("node not found")
		}

		if len(n.store) > 0 {
			return errors.New("must remove store first")
		}
		return nil
	}()

	if nil == err {
		delete(s.nodes, int(p.Msg.NodeID))
		pd.pState.deployment.version++
	}

	if nil != p.reply {
		p.reply(err)
	}
}

func (p *ProposalRemNode) replay(pd *pd) {
	p.apply(pd)
}

func (p *pd) onAddNode(from *net.UDPAddr, m *snet.Message) {
	msg := m.Msg.(*sproto.AddNode)
	resp := &sproto.AddNodeResp{}

	err := func() error {
		if nil == p.pState.deployment {
			return errors.New("must init deployment first")
		}

		_, ok := p.pState.deployment.sets[int(msg.SetID)]
		if !ok {
			return errors.New("set not found")
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
		return nil
	}()

	GetSugar().Infof("onAddNode %v", err)

	if nil != err {
		resp.Ok = false
		resp.Reason = err.Error()
		p.udp.SendTo(from, snet.MakeMessage(m.Context, resp))
	} else {
		p.issueProposal(&ProposalAddNode{
			Msg: msg,
			proposalBase: proposalBase{
				reply: p.makeReplyFunc(from, m, resp),
			},
		})
	}
}

func (p *pd) onRemNode(from *net.UDPAddr, m *snet.Message) {
	msg := m.Msg.(*sproto.RemNode)
	resp := &sproto.RemNodeResp{}

	err := func() error {
		if nil == p.pState.deployment {
			return errors.New("no deployment")
		}

		s, ok := p.pState.deployment.sets[int(msg.SetID)]
		if !ok {
			return errors.New("set not found")
		}

		n, ok := s.nodes[int(msg.NodeID)]
		if !ok {
			return errors.New("node not found")
		}

		if len(n.store) > 0 {
			return errors.New("must remove store first")
		}

		return nil
	}()

	if nil != err {
		resp.Ok = false
		resp.Reason = err.Error()
		p.udp.SendTo(from, snet.MakeMessage(m.Context, resp))
	} else {

		p.issueProposal(&ProposalRemNode{
			Msg: msg,
			proposalBase: proposalBase{
				reply: p.makeReplyFunc(from, m, resp),
			},
		})
	}
}
