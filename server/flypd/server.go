package flypd

import (
	//"errors"
	"fmt"
	"github.com/gogo/protobuf/proto"
	sproto "github.com/sniperHW/flyfish/server/proto"
	"net"
	//"net/url"
	"reflect"
)

func (p *pd) registerMsgHandler(msg proto.Message, handler func(*net.UDPAddr, proto.Message)) {
	if nil != msg {
		p.msgHandler[reflect.TypeOf(msg)] = handler
	}
}

func (p *pd) onMsg(from *net.UDPAddr, msg proto.Message) {
	GetSugar().Infof("onMsg %v", msg)
	if h, ok := p.msgHandler[reflect.TypeOf(msg)]; ok {
		h(from, msg)
	}
}

func (p *pd) onInstallDeployment(from *net.UDPAddr, m proto.Message) {

	msg := m.(*sproto.InstallDeployment)

	if nil != p.deployment {
		p.udp.SendTo(from, &sproto.InstallDeploymentResp{
			Ok:     false,
			Reason: "already install",
		})
		return
	}

	d := &deployment{}
	if err := d.loadFromPB(msg.Sets); nil != err {
		p.udp.SendTo(from, &sproto.InstallDeploymentResp{
			Ok:     false,
			Reason: err.Error(),
		})
		return
	}

	err := p.issueProposal(&ProposalInstallDeployment{
		d: d,
		proposalBase: &proposalBase{
			pd: p,
			reply: func(err ...error) {
				if len(err) == 0 {
					p.udp.SendTo(from, &sproto.InstallDeploymentResp{
						Ok: true,
					})
				} else {
					p.udp.SendTo(from, &sproto.InstallDeploymentResp{
						Ok:     false,
						Reason: err[0].Error(),
					})
				}
			},
		},
	})

	if nil != err {
		p.udp.SendTo(from, &sproto.InstallDeploymentResp{
			Ok:     false,
			Reason: err.Error(),
		})
	}

}

func (p *pd) onRemSet(from *net.UDPAddr, m proto.Message) {
	msg := m.(*sproto.RemSet)
	if nil == p.deployment {
		p.udp.SendTo(from, &sproto.RemSetResp{
			Ok:     false,
			Reason: "no deployment",
		})
		return
	}

	s, ok := p.deployment.sets[int(msg.SetID)]
	if !ok {
		p.udp.SendTo(from, &sproto.RemSetResp{
			Ok:     false,
			Reason: "set not exists",
		})
		return
	}

	//只有当s中所有的store都不存在slot时才能移除
	for _, v := range s.stores {
		if len(v.slots.GetOpenBits()) != 0 {
			p.udp.SendTo(from, &sproto.RemSetResp{
				Ok:     false,
				Reason: fmt.Sprintf("there are slots in store:%d", v.id),
			})
			return
		}
	}

	err := p.issueProposal(&ProposalRemSet{
		setID: int(msg.SetID),
		proposalBase: &proposalBase{
			pd: p,
			reply: func(err ...error) {
				if len(err) == 0 {
					p.udp.SendTo(from, &sproto.RemSetResp{
						Ok: true,
					})
				} else {
					p.udp.SendTo(from, &sproto.RemSetResp{
						Ok:     false,
						Reason: err[0].Error(),
					})
				}
			},
		},
	})

	if nil != err {
		p.udp.SendTo(from, &sproto.RemSetResp{
			Ok:     false,
			Reason: err.Error(),
		})
	}

}

func (p *pd) onAddSet(from *net.UDPAddr, m proto.Message) {
	msg := m.(*sproto.AddSet)
	if nil == p.deployment {
		p.udp.SendTo(from, &sproto.AddSetResp{
			Ok:     false,
			Reason: "no deployment",
		})
		return
	}

	_, ok := p.deployment.sets[int(msg.Set.SetID)]
	if ok {
		p.udp.SendTo(from, &sproto.AddSetResp{
			Ok:     false,
			Reason: "set already exists",
		})
		return
	}

	//检查node是否有冲突
	nodeIDS := map[int]bool{}
	nodeServices := map[string]bool{}
	nodeInters := map[string]bool{}

	for _, v := range p.deployment.sets {
		for _, vv := range v.nodes {
			nodeIDS[vv.id] = true
			nodeServices[fmt.Sprintf("%s:%d", vv.host, vv.servicePort)] = true
			nodeInters[fmt.Sprintf("%s:%d", vv.host, vv.servicePort)] = true
		}
	}

	for _, v := range msg.Set.Nodes {
		if nodeIDS[int(v.NodeID)] {
			p.udp.SendTo(from, &sproto.AddSetResp{
				Ok:     false,
				Reason: fmt.Sprintf("duplicate node:%d", v.NodeID),
			})
			return
		}

		if nodeServices[fmt.Sprintf("%s:%d", v.Host, v.ServicePort)] {
			p.udp.SendTo(from, &sproto.AddSetResp{
				Ok:     false,
				Reason: fmt.Sprintf("duplicate service:%s:%d", v.Host, v.ServicePort),
			})
			return
		}

		if nodeInters[fmt.Sprintf("%s:%d", v.Host, v.InterPort)] {
			p.udp.SendTo(from, &sproto.AddSetResp{
				Ok:     false,
				Reason: fmt.Sprintf("duplicate inter:%s:%d", v.Host, v.InterPort),
			})
			return
		}

		nodeIDS[int(v.NodeID)] = true
		nodeServices[fmt.Sprintf("%s:%d", v.Host, v.ServicePort)] = true
		nodeInters[fmt.Sprintf("%s:%d", v.Host, v.InterPort)] = true
	}

	err := p.issueProposal(&ProposalAddSet{
		msg: msg,
		proposalBase: &proposalBase{
			pd: p,
			reply: func(err ...error) {
				if len(err) == 0 {
					p.udp.SendTo(from, &sproto.AddSetResp{
						Ok: true,
					})
				} else {
					p.udp.SendTo(from, &sproto.AddSetResp{
						Ok:     false,
						Reason: err[0].Error(),
					})
				}
			},
		},
	})

	if nil != err {
		p.udp.SendTo(from, &sproto.AddSetResp{
			Ok:     false,
			Reason: err.Error(),
		})
	}

}

func (p *pd) onAddNode(from *net.UDPAddr, m proto.Message) {
	msg := m.(*sproto.AddNode)
	if nil == p.deployment {
		p.udp.SendTo(from, &sproto.AddNodeResp{
			Ok:     false,
			Reason: "no deployment",
		})
		return
	}

	_, ok := p.addingNode[int(msg.NodeID)]
	if ok {
		p.udp.SendTo(from, &sproto.AddNodeResp{
			Ok: true,
		})
		return
	}

	s, ok := p.deployment.sets[int(msg.SetID)]
	if !ok {
		p.udp.SendTo(from, &sproto.AddNodeResp{
			Ok:     false,
			Reason: "set not found",
		})
		return
	}

	_, ok = s.nodes[int(msg.NodeID)]
	if ok {
		p.udp.SendTo(from, &sproto.AddNodeResp{
			Ok:     false,
			Reason: "duplicate node id",
		})
		return
	}

	//检查是否存在重复服务地址
	for _, v := range p.deployment.sets {
		for _, vv := range v.nodes {
			if vv.host == msg.Host && vv.servicePort == int(msg.ServicePort) {
				p.udp.SendTo(from, &sproto.AddNodeResp{
					Ok:     false,
					Reason: "duplicate service addr",
				})
				return
			}

			if vv.host == msg.Host && vv.interPort == int(msg.InterPort) {
				p.udp.SendTo(from, &sproto.AddNodeResp{
					Ok:     false,
					Reason: "duplicate inter addr",
				})
				return
			}
		}
	}

	err := p.issueProposal(&ProposalAddNode{
		msg:        msg,
		sendNotify: true,
		proposalBase: &proposalBase{
			pd: p,
			reply: func(err ...error) {
				if len(err) == 0 {
					p.udp.SendTo(from, &sproto.AddNodeResp{
						Ok: true,
					})
				} else {
					p.udp.SendTo(from, &sproto.AddNodeResp{
						Ok:     false,
						Reason: err[0].Error(),
					})
				}
			},
		},
	})

	if nil != err {
		p.udp.SendTo(from, &sproto.AddNodeResp{
			Ok:     false,
			Reason: err.Error(),
		})
	}

}

func (p *pd) onNotifyAddNodeResp(from *net.UDPAddr, m proto.Message) {

	msg := m.(*sproto.NotifyAddNodeResp)
	an, ok := p.addingNode[int(msg.NodeID)]
	if ok {

		find := false
		for i := 0; i < len(an.OkStores); i++ {
			if an.OkStores[i] == int(msg.Store) {
				find = true
				break
			}
		}

		if !find {
			p.issueProposal(&ProposalNotifyAddNodeResp{
				msg: msg,
				proposalBase: &proposalBase{
					pd: p,
				},
			})
		}
	}
}

func (p *pd) onRemNode(from *net.UDPAddr, m proto.Message) {
	msg := m.(*sproto.RemNode)
	if nil == p.deployment {
		p.udp.SendTo(from, &sproto.RemNodeResp{
			Ok:     false,
			Reason: "no deployment",
		})
		return
	}

	_, ok := p.removingNode[int(msg.NodeID)]
	if ok {
		p.udp.SendTo(from, &sproto.RemNodeResp{
			Ok: true,
		})
		return
	}

	s, ok := p.deployment.sets[int(msg.SetID)]
	if !ok {
		p.udp.SendTo(from, &sproto.RemNodeResp{
			Ok:     false,
			Reason: "set not found",
		})
		return
	}

	_, ok = s.nodes[int(msg.NodeID)]
	if !ok {
		p.udp.SendTo(from, &sproto.RemNodeResp{
			Ok:     false,
			Reason: "node not found",
		})
		return
	}

	//不允许将节点数量减少到KvNodePerSet以下
	if len(s.nodes)-1 < KvNodePerSet {
		if !ok {
			p.udp.SendTo(from, &sproto.RemNodeResp{
				Ok:     false,
				Reason: fmt.Sprintf("cannot remove node,should keep %d node per set", KvNodePerSet),
			})
			return
		}
	}

	err := p.issueProposal(&ProposalRemNode{
		msg:        msg,
		sendNotify: true,
		proposalBase: &proposalBase{
			pd: p,
			reply: func(err ...error) {
				if len(err) == 0 {
					p.udp.SendTo(from, &sproto.RemNodeResp{
						Ok: true,
					})
				} else {
					p.udp.SendTo(from, &sproto.RemNodeResp{
						Ok:     false,
						Reason: err[0].Error(),
					})
				}
			},
		},
	})

	if nil != err {
		p.udp.SendTo(from, &sproto.RemNodeResp{
			Ok:     false,
			Reason: err.Error(),
		})
	}
}

func (p *pd) onNotifyRemNodeResp(from *net.UDPAddr, m proto.Message) {
	msg := m.(*sproto.NotifyRemNodeResp)
	rn, ok := p.removingNode[int(msg.NodeID)]
	if ok {
		find := false
		for i := 0; i < len(rn.OkStores); i++ {
			if rn.OkStores[i] == int(msg.Store) {
				find = true
				break
			}
		}

		if !find {
			p.issueProposal(&ProposalNotifyRemNodeResp{
				msg: msg,
				proposalBase: &proposalBase{
					pd: p,
				},
			})
		}
	}
}

func (p *pd) onNotifySlotTransOutResp(from *net.UDPAddr, m proto.Message) {
	msg := m.(*sproto.NotifySlotTransOutResp)
	if t, ok := p.slotTransfer[int(msg.Slot)]; ok {
		if !t.StoreTransferOutOk {
			p.issueProposal(&ProposalNotifySlotTransOutResp{
				slot: int(msg.Slot),
				proposalBase: &proposalBase{
					pd: p,
				},
			})

		}
	}
}

func (p *pd) onNotifySlotTransInResp(from *net.UDPAddr, m proto.Message) {
	msg := m.(*sproto.NotifySlotTransInResp)
	if _, ok := p.slotTransfer[int(msg.Slot)]; ok {
		p.issueProposal(&ProposalNotifySlotTransInResp{
			slot: int(msg.Slot),
			proposalBase: &proposalBase{
				pd: p,
			},
		})
	}
}

func (p *pd) initMsgHandler() {
	p.registerMsgHandler(&sproto.InstallDeployment{}, p.onInstallDeployment)
	p.registerMsgHandler(&sproto.AddSet{}, p.onAddSet)
	p.registerMsgHandler(&sproto.RemSet{}, p.onRemSet)
	p.registerMsgHandler(&sproto.AddNode{}, p.onAddNode)
	p.registerMsgHandler(&sproto.NotifyAddNodeResp{}, p.onNotifyAddNodeResp)
	p.registerMsgHandler(&sproto.RemNode{}, p.onRemNode)
	p.registerMsgHandler(&sproto.NotifyRemNodeResp{}, p.onNotifyRemNodeResp)
	p.registerMsgHandler(&sproto.NotifySlotTransOutResp{}, p.onNotifySlotTransOutResp)
	p.registerMsgHandler(&sproto.NotifySlotTransInResp{}, p.onNotifySlotTransInResp)
}