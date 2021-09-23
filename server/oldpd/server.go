package flypd

import (
	"errors"
	"fmt"
	"github.com/gogo/protobuf/proto"
	sproto "github.com/sniperHW/flyfish/server/proto"
	"net"
	"net/url"
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

func (p *pd) onKvnodeBoot(from *net.UDPAddr, m proto.Message) {
	msg := m.(*sproto.KvnodeBoot)
	resp := &sproto.KvnodeBootResp{}

	n, ok := p.kvnodes[int(msg.GetNodeID())]
	if !ok {
		resp.Ok = false
		resp.Reason = "invaild node id"
	} else {
		resp.Ok = true
		resp.Service = n.service
		resp.UdpService = n.udpService
		resp.RaftService = n.raftService
		for _, v := range n.stores {
			resp.Stores = append(resp.Stores, &sproto.StoreInfo{
				Id:          int32(v.id),
				Slots:       v.slots.ToJson(),
				RaftCluster: v.clusterStr,
			})
		}
	}

	p.udp.SendTo(from, resp)
}

func (p *pd) onAddKvnode(from *net.UDPAddr, m proto.Message) {
	msg := m.(*sproto.AddKvnode)
	resp := &sproto.AddKvnodeResp{
		Seqno: msg.GetSeqno(),
	}
	id := int(msg.GetNodeId())
	n, ok := p.kvnodes[id]
	if ok {
		resp.Ok = true
		p.udp.SendTo(from, resp)
	} else {

		checkKvnode := func(service, raftService, udpService string) error {
			//重复检查
			for _, vv := range p.kvnodes {

				if service == vv.service {
					return errors.New(fmt.Sprintf("repeated service:%s", service))
				}

				if raftService == vv.raftService {
					return errors.New(fmt.Sprintf("repeated RaftService:%s", raftService))
				}

				if udpService == vv.udpService {
					return errors.New(fmt.Sprintf("repeated UdpService:%s", udpService))
				}
			}
			return nil
		}

		if err := checkKvnode(msg.GetService(), msg.GetRaftService(), msg.GetUdpService()); nil != err {
			resp.Ok = false
			resp.Reason = err.Error()
			p.udp.SendTo(from, resp)
			return
		}

		n = &kvnode{
			id:          id,
			service:     msg.GetService(),
			raftService: msg.GetRaftService(),
			udpService:  msg.GetUdpService(),
			stores:      map[int]*store{},
		}

		if _, err := net.ResolveTCPAddr("tcp", n.service); nil != err {
			resp.Ok = false
			resp.Reason = fmt.Sprintf("service %s is invaild", n.service)
			p.udp.SendTo(from, resp)
			return
		}

		if _, err := url.Parse(n.raftService); nil != err {
			resp.Ok = false
			resp.Reason = fmt.Sprintf("raftService %s is invaild", n.raftService)
			p.udp.SendTo(from, resp)
			return
		}

		if udpAddr, err := net.ResolveUDPAddr("udp", n.udpService); nil != err {
			resp.Ok = false
			resp.Reason = fmt.Sprintf("udpService %s is invaild", n.udpService)
			p.udp.SendTo(from, resp)
			return
		} else {
			n.udpAddr = udpAddr
		}

		//发起proposal
		p.issueProposal(&addKvnodeProposal{
			n: n,
			proposalBase: &proposalBase{
				pd: p,
				reply: func(err ...error) {
					if len(err) == 0 {
						resp.Ok = true
						p.udp.SendTo(from, resp)
					} else {
						resp.Ok = false
						resp.Reason = err[0].Error()
						p.udp.SendTo(from, resp)
					}
				},
			},
		})
	}
}

func (p *pd) onRemKvnode(from *net.UDPAddr, m proto.Message) {
	msg := m.(*sproto.RemKvnode)
	resp := &sproto.RemKvnodeResp{
		Seqno: msg.GetSeqno(),
	}
	id := int(msg.GetNodeId())
	n, ok := p.kvnodes[id]
	if !ok {
		resp.Ok = true
		p.udp.SendTo(from, resp)
	} else if len(n.stores) > 0 {
		//还有关联的store,不能直接移除
		resp.Ok = false
		resp.Reason = "remove store on this kvnode"
		p.udp.SendTo(from, resp)
	} else {
		p.issueProposal(&remKvnodeProposal{
			n: n,
			proposalBase: &proposalBase{
				pd: p,
				reply: func(err ...error) {
					if len(err) == 0 {
						resp.Ok = true
						p.udp.SendTo(from, resp)
					} else {
						resp.Ok = false
						resp.Reason = err[0].Error()
						p.udp.SendTo(from, resp)
					}
				},
			},
		})
	}
}

func (p *pd) onAddStore(from *net.UDPAddr, m proto.Message) {
	msg := m.(*sproto.AddStore)
	resp := &sproto.AddStoreResp{
		Seqno: msg.GetSeqno(),
	}
	id := int(msg.GetId())
	_, ok := p.stores[id]
	if ok {
		resp.Ok = true
		p.udp.SendTo(from, resp)
	} else {
		p.issueProposal(&addStoreProposal{
			s: id,
			proposalBase: &proposalBase{
				pd: p,
				reply: func(err ...error) {
					if len(err) == 0 {
						resp.Ok = true
						p.udp.SendTo(from, resp)
					} else {
						resp.Ok = false
						resp.Reason = err[0].Error()
						p.udp.SendTo(from, resp)
					}
				},
			},
		})
	}
}

func (p *pd) onRemStore(from *net.UDPAddr, m proto.Message) {
	msg := m.(*sproto.RemStore)
	resp := &sproto.RemStoreResp{
		Seqno: msg.GetSeqno(),
	}
	id := int(msg.GetId())
	s, ok := p.stores[id]
	if !ok || s.removing {
		resp.Ok = true
		p.udp.SendTo(from, resp)
	} else if len(s.kvnodes) != 0 {
		//store上还有关联的kvnode，不能直接删除store
		resp.Ok = false
		resp.Reason = "remove kvnode associative with this store"
		p.udp.SendTo(from, resp)
	} else if len(p.stores) == 1 {
		//唯一的store不能移除，否则slot将无处安身
		resp.Ok = false
		resp.Reason = "can't remove the only store"
		p.udp.SendTo(from, resp)
	} else {
		p.issueProposal(&remStoreProposal{
			s: s,
			proposalBase: &proposalBase{
				pd: p,
				reply: func(err ...error) {
					if len(err) == 0 {
						resp.Ok = true
						p.udp.SendTo(from, resp)
					} else {
						resp.Ok = false
						resp.Reason = err[0].Error()
						p.udp.SendTo(from, resp)
					}
				},
			},
		})
	}
}

func (p *pd) onKvnodeAddStore(from *net.UDPAddr, m proto.Message) {
	msg := m.(*sproto.KvnodeAddStore)
	resp := &sproto.KvnodeAddStoreResp{
		Seqno: msg.GetSeqno(),
	}
	nodeId := int(msg.GetNodeId())
	storeId := int(msg.GetStoreId())
	n, ok := p.kvnodes[nodeId]
	if !ok {
		resp.Ok = false
		resp.Reason = "kvnode not exist"
		p.udp.SendTo(from, resp)
		return
	}
	s, ok := p.stores[storeId]
	if !ok {
		resp.Ok = false
		resp.Reason = "store not exist"
		p.udp.SendTo(from, resp)
		return
	}
	if s.kvnodes[nodeId] != nil {
		resp.Ok = true
		p.udp.SendTo(from, resp)
		return
	}

	transID := makeTransactionNodeStoreID(s.id, n.id)

	var trans *nodeStoreTransaction

	if trans = p.transNodeStore[transID]; nil == trans {
		trans = p.tmpTransNodeStore[transID]
	}

	if nil != trans {
		//从n删除s的事务尚未执行完毕
		resp.Ok = false
		resp.Reason = "a store trans is doing"
		p.udp.SendTo(from, resp)
		return
	}

	trans = &nodeStoreTransaction{
		TransID:       transID,
		Type:          sproto.KvnodeStoreTransType_TransAddStore,
		NodeId:        n.id,
		StoreId:       s.id,
		pd:            p,
		GotLeaderResp: len(s.kvnodes) == 0, //如果添加第一个kvnode,将不存在leader
	}

	p.issueProposal(&kvnodeStoreTransProposal{
		trans: trans,
		proposalBase: &proposalBase{
			pd: p,
			reply: func(err ...error) {
				if len(err) == 0 {
					resp.Ok = true
					p.udp.SendTo(from, resp)
				} else {
					resp.Ok = false
					resp.Reason = err[0].Error()
					p.udp.SendTo(from, resp)
				}
			},
		},
	})
}

func (p *pd) onKvnodeRemStore(from *net.UDPAddr, m proto.Message) {
	msg := m.(*sproto.KvnodeRemStore)
	resp := &sproto.KvnodeRemStoreResp{
		Seqno: msg.GetSeqno(),
	}
	nodeId := int(msg.GetNodeId())
	storeId := int(msg.GetStoreId())

	n, ok := p.kvnodes[nodeId]
	if !ok {
		resp.Ok = false
		resp.Reason = "kvnode not exist"
		p.udp.SendTo(from, resp)
		return
	}
	s, ok := p.stores[storeId]
	if !ok {
		resp.Ok = false
		resp.Reason = "store not exist"
		p.udp.SendTo(from, resp)
		return
	}
	if s.kvnodes[nodeId] == nil {
		resp.Ok = true
		p.udp.SendTo(from, resp)
		return
	}

	transID := makeTransactionNodeStoreID(s.id, n.id)

	var trans *nodeStoreTransaction

	if trans = p.transNodeStore[transID]; nil == trans {
		trans = p.tmpTransNodeStore[transID]
	}

	if nil != trans {
		resp.Ok = false
		resp.Reason = "a store trans is doing"
		p.udp.SendTo(from, resp)
		return
	}

	trans = &nodeStoreTransaction{
		TransID: transID,
		Type:    sproto.KvnodeStoreTransType_TransRemStore,
		NodeId:  n.id,
		StoreId: s.id,
		pd:      p,
	}

	p.issueProposal(&kvnodeStoreTransProposal{
		trans: trans,
		proposalBase: &proposalBase{
			pd: p,
			reply: func(err ...error) {
				if len(err) == 0 {
					resp.Ok = true
					p.udp.SendTo(from, resp)
				} else {
					resp.Ok = false
					resp.Reason = err[0].Error()
					p.udp.SendTo(from, resp)
				}
			},
		},
	})
}

func (p *pd) onNotifyKvnodeStoreTransResp(from *net.UDPAddr, m proto.Message) {
	msg := m.(*sproto.NotifyKvnodeStoreTransResp)
	if trans, ok := p.transNodeStore[msg.GetTransID()]; ok {
		p.issueProposal(&kvnodeStoreTransRespProposal{
			nodeId:   int(msg.GetNodeId()),
			isLeader: msg.GetIsLeader(),
			trans:    trans,
			proposalBase: &proposalBase{
				pd: p,
			},
		})
	}
}

func (p *pd) onSlotTransferPrepareAck(from *net.UDPAddr, m proto.Message) {
	msg := m.(*sproto.SlotTransferPrepareAck)

	trans, ok := p.transSlotTransfer[msg.GetTransID()]

	if !ok || trans.isCancel() {
		p.udp.SendTo(from, &sproto.SlotTransferCancel{
			TransID: msg.GetTransID(),
			StoreID: msg.GetStoreID(),
		})
	} else if trans.isCommit() {
		p.udp.SendTo(from, &sproto.SlotTransferCommit{
			TransID: msg.GetTransID(),
			StoreID: msg.GetStoreID(),
		})
	} else if trans.isPrepare() {
		if !msg.GetOk() {
			trans.timer.Cancel()
			trans.timer = nil
			trans.tmpState = slotTransferCancel
			trans.pd.issueProposal(&slotTransferCancelProposal{
				trans: trans,
				proposalBase: &proposalBase{
					pd: trans.pd,
				},
			})
		} else {
			storeId := int(msg.GetStoreID())
			if trans.OutStoreID == storeId {
				trans.outAgree = true
			} else if trans.InStoreID == storeId {
				trans.inAgree = true
			}

			if trans.inAgree && trans.outAgree {
				trans.tmpState = slotTransferCommit
				//双方都通过,提交
				trans.timer.Cancel()
				trans.timer = nil
				p.issueProposal(&slotTransferCommitProposal{
					trans: trans,
					proposalBase: &proposalBase{
						pd: trans.pd,
					},
				})
			}
		}
	}
}

func (p *pd) initMsgHandler() {
	p.registerMsgHandler(&sproto.KvnodeBoot{}, p.onKvnodeBoot)
	p.registerMsgHandler(&sproto.AddKvnode{}, p.onAddKvnode)
	p.registerMsgHandler(&sproto.RemKvnode{}, p.onRemKvnode)
	p.registerMsgHandler(&sproto.AddStore{}, p.onAddStore)
	p.registerMsgHandler(&sproto.RemStore{}, p.onRemStore)
	p.registerMsgHandler(&sproto.KvnodeAddStore{}, p.onKvnodeAddStore)
	p.registerMsgHandler(&sproto.KvnodeRemStore{}, p.onKvnodeRemStore)
	p.registerMsgHandler(&sproto.NotifyKvnodeStoreTransResp{}, p.onNotifyKvnodeStoreTransResp)
	p.registerMsgHandler(&sproto.SlotTransferPrepareAck{}, p.onSlotTransferPrepareAck)
}
