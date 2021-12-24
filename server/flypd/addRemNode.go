package flypd

import (
	"encoding/json"
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
	s := p.pd.pState.deployment.sets[int(p.msg.SetID)]
	if _, ok := s.nodes[int(p.msg.NodeID)]; !ok {
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
		p.reply(nil)
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
	s := p.pd.pState.deployment.sets[int(p.msg.SetID)]
	if _, ok := s.nodes[int(p.msg.NodeID)]; ok {
		delete(s.nodes, int(p.msg.NodeID))
		p.pd.pState.deployment.version++
	}

	if nil != p.reply {
		p.reply(nil)
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

	reply, reason := func() (bool, string) {
		if nil == p.pState.deployment {
			return true, "must init deployment first"
		}

		s, ok := p.pState.deployment.sets[int(msg.SetID)]
		if !ok {
			return true, "set not found"
		}

		_, ok = s.nodes[int(msg.NodeID)]
		if ok {
			if s.id == int(msg.SetID) {
				return true, ""
			} else {
				return true, "duplicate node id"
			}
		}

		//检查是否存在重复服务地址
		for _, v := range p.pState.deployment.sets {
			for _, vv := range v.nodes {
				if vv.host == msg.Host && vv.servicePort == int(msg.ServicePort) {
					return true, "duplicate service addr"
				}

				if vv.host == msg.Host && vv.raftPort == int(msg.RaftPort) {
					return true, "duplicate raft addr"
				}
			}
		}
		return false, ""
	}()

	if reply {
		resp.Ok = (reason == "")
		resp.Reason = reason
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

	reply, reason := func() (bool, string) {
		if nil == p.pState.deployment {
			return true, "no deployment"
		}

		s, ok := p.pState.deployment.sets[int(msg.SetID)]
		if !ok {
			return true, "set not found"
		}

		n, ok := s.nodes[int(msg.NodeID)]
		if !ok {
			return true, "node not found"
		}

		if len(n.store) > 0 {
			return true, "must remove store first"
		}

		return false, ""
	}()

	if reply {
		resp.Ok = (reason == "")
		resp.Reason = reason
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
