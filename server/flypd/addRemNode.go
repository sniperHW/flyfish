package flypd

import (
	"encoding/json"
	"errors"
	"github.com/sniperHW/flyfish/pkg/buffer"
	snet "github.com/sniperHW/flyfish/server/net"
	sproto "github.com/sniperHW/flyfish/server/proto"
	"net"
)

type ProposalAddNode struct {
	*proposalBase
	msg *sproto.AddNode
}

func (p *ProposalAddNode) Serilize(b []byte) []byte {
	b = buffer.AppendByte(b, byte(proposalAddNode))
	bb, err := json.Marshal(p.msg)
	if nil != err {
		panic(err)
	}
	return buffer.AppendBytes(b, bb)
}

func (p *ProposalAddNode) apply() {
	var s *set
	var ok bool

	err := func() error {
		s, ok = p.pd.pState.deployment.sets[int(p.msg.SetID)]
		if !ok {
			return errors.New("set not found")
		}

		for _, v := range p.pd.pState.deployment.sets {
			for _, vv := range v.nodes {
				if vv.id == int(p.msg.SetID) {
					return errors.New("duplicate node id")
				}

				if vv.host == p.msg.Host && vv.servicePort == int(p.msg.ServicePort) {
					return errors.New("duplicate service addr")
				}

				if vv.host == p.msg.Host && vv.raftPort == int(p.msg.RaftPort) {
					return errors.New("duplicate raft addr")
				}
			}
		}
		return nil
	}()

	if nil == err {
		n := &kvnode{
			id:          int(p.msg.NodeID),
			host:        p.msg.Host,
			servicePort: int(p.msg.ServicePort),
			raftPort:    int(p.msg.RaftPort),
			set:         s,
			store:       map[int]*FlyKvStoreState{},
		}
		s.nodes[int(p.msg.NodeID)] = n
		p.pd.pState.deployment.version++
	}

	if nil != p.reply {
		p.reply(err)
	}
}

func (p *pd) replayAddNode(reader *buffer.BufferReader) error {
	var msg sproto.AddNode
	if err := json.Unmarshal(reader.GetAll(), &msg); nil != err {
		return err
	}

	pa := &ProposalAddNode{
		proposalBase: &proposalBase{
			pd: p,
		},
		msg: &msg,
	}
	pa.apply()
	return nil
}

type ProposalRemNode struct {
	*proposalBase
	msg *sproto.RemNode
}

func (p *ProposalRemNode) Serilize(b []byte) []byte {
	b = buffer.AppendByte(b, byte(proposalRemNode))
	bb, err := json.Marshal(p.msg)
	if nil != err {
		panic(err)
	}
	return buffer.AppendBytes(b, bb)
}

func (p *ProposalRemNode) apply() {

	var s *set
	var ok bool

	err := func() error {

		s, ok = p.pd.pState.deployment.sets[int(p.msg.SetID)]
		if !ok {
			return errors.New("set not found")
		}

		n, ok := s.nodes[int(p.msg.NodeID)]
		if !ok {
			return errors.New("node not found")
		}

		if len(n.store) > 0 {
			return errors.New("must remove store first")
		}
		return nil
	}()

	if nil == err {
		delete(s.nodes, int(p.msg.NodeID))
		p.pd.pState.deployment.version++
	}

	if nil != p.reply {
		p.reply(err)
	}
}

func (p *pd) replayRemNode(reader *buffer.BufferReader) error {
	var msg sproto.RemNode
	if err := json.Unmarshal(reader.GetAll(), &msg); nil != err {
		return err
	}

	pr := &ProposalRemNode{
		proposalBase: &proposalBase{
			pd: p,
		},
		msg: &msg,
	}
	pr.apply()
	return nil
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
				if vv.id == int(msg.SetID) {
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

	if nil != err {
		resp.Ok = false
		resp.Reason = err.Error()
		p.udp.SendTo(from, snet.MakeMessage(m.Context, resp))
	} else {

		p.issueProposal(&ProposalAddNode{
			msg: msg,
			proposalBase: &proposalBase{
				pd:    p,
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
			msg: msg,
			proposalBase: &proposalBase{
				pd:    p,
				reply: p.makeReplyFunc(from, m, resp),
			},
		})
	}
}
