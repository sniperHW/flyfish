package flypd

import (
	"fmt"
	"github.com/gogo/protobuf/proto"
	snet "github.com/sniperHW/flyfish/server/net"
	sproto "github.com/sniperHW/flyfish/server/proto"
	"math"
	"net"
	"reflect"
	"strings"
	"time"
)

func (p *pd) registerMsgHandler(msg proto.Message, handler func(*net.UDPAddr, *snet.Message)) {
	if nil != msg {
		p.msgHandler[reflect.TypeOf(msg)] = handler
	}
}

func (p *pd) onMsg(from *net.UDPAddr, msg *snet.Message) {
	if h, ok := p.msgHandler[reflect.TypeOf(msg.Msg)]; ok {
		h(from, msg)
	}
}

func (p *pd) onKvnodeBoot(from *net.UDPAddr, m *snet.Message) {
	if nil != p.pState.deployment {
		msg := m.Msg.(*sproto.KvnodeBoot)
		node := p.getNode(msg.NodeID)
		if nil == node {
			p.udp.SendTo(from, snet.MakeMessage(m.Context,
				&sproto.KvnodeBootResp{
					Ok:     false,
					Reason: fmt.Sprintf("node:%d not in deployment", msg.NodeID),
				}))
			return
		}

		resp := &sproto.KvnodeBootResp{
			Ok:          true,
			ServiceHost: node.host,
			SetID:       int32(node.set.id),
			ServicePort: int32(node.servicePort),
			RaftPort:    int32(node.raftPort),
			MetaVersion: p.pState.Meta.Version,
			Meta:        p.pState.Meta.MetaBytes,
		}

		for storeId, _ := range node.store {
			store := node.set.stores[storeId]
			raftCluster := []string{}
			for _, n := range node.set.nodes {
				if n.isVoter(store.id) {
					raftCluster = append(raftCluster, fmt.Sprintf("%d@http://%s:%d@voter", n.id, n.host, n.raftPort))
				} else if n.isLearner(store.id) {
					raftCluster = append(raftCluster, fmt.Sprintf("%d@http://%s:%d@learner", n.id, n.host, n.raftPort))
				}
			}
			s := &sproto.StoreInfo{
				Id:          int32(store.id),
				Slots:       store.slots.ToJson(),
				RaftCluster: strings.Join(raftCluster, ","),
			}
			resp.Stores = append(resp.Stores, s)
		}

		p.udp.SendTo(from, snet.MakeMessage(m.Context, resp))
	} else {
		GetSugar().Errorf("onKvnodeBoot but deployment == nil")
	}
}

func (p *pd) onQueryRouteInfo(from *net.UDPAddr, m *snet.Message) {
	msg := m.Msg.(*sproto.QueryRouteInfo)
	if nil != p.pState.deployment {
		resp := p.pState.deployment.queryRouteInfo(msg)
		p.udp.SendTo(from, snet.MakeMessage(m.Context, resp))
	}
}

func (p *pd) onGetFlyGateList(from *net.UDPAddr, m *snet.Message) {
	resp := &sproto.GetFlyGateListResp{}
	for _, v := range p.flygateMgr.flygateMap {
		resp.List = append(resp.List, &sproto.Flygate{
			Service:      v.service,
			MsgPerSecond: int32(v.msgPerSecond),
		})
	}
	p.udp.SendTo(from, snet.MakeMessage(m.Context, resp))
}

func (p *pd) onFlyGateHeartBeat(from *net.UDPAddr, m *snet.Message) {
	msg := m.Msg.(*sproto.FlyGateHeartBeat)
	p.flygateMgr.onHeartBeat(msg.GateService, int(msg.MsgPerSecond))
}

func (p *pd) changeFlyGate(from *net.UDPAddr, m *snet.Message) {
	msg := m.Msg.(*sproto.ChangeFlyGate)
	currentGate := p.flygateMgr.flygateMap[msg.CurrentGate]

	min := int(math.MaxInt32)
	var minGate *flygate

	average := 0

	for _, v := range p.flygateMgr.flygateMap {
		average += v.msgPerSecond
		if v.msgPerSecond < min {
			min = v.msgPerSecond
			minGate = v
		}

	}

	average = average / len(p.flygateMgr.flygateMap)

	var target *flygate

	msgSendPerSecond := int(msg.MsgSendPerSecond)

	if nil != currentGate && currentGate.msgPerSecond-msgSendPerSecond > average {
		if float64(minGate.msgPerSecond+msgSendPerSecond)/float64(average) < 1.05 {
			target = minGate
		}
	}

	if nil != target && target != currentGate {
		target.msgPerSecond += msgSendPerSecond
		currentGate.msgPerSecond -= msgSendPerSecond
		p.udp.SendTo(from, snet.MakeMessage(m.Context, &sproto.ChangeFlyGateResp{Ok: true, Service: target.service}))
	} else {
		p.udp.SendTo(from, snet.MakeMessage(m.Context, &sproto.ChangeFlyGateResp{Ok: false}))
	}
}

func (p *pd) onGetSetStatus(from *net.UDPAddr, m *snet.Message) {
	if nil != p.pState.deployment {
		resp := &sproto.GetSetStatusResp{}
		for _, v := range p.pState.deployment.sets {
			s := &sproto.SetStatus{
				SetID:     int32(v.id),
				MarkClear: v.markClear,
			}

			for _, vv := range v.nodes {
				n := &sproto.KvnodeStatus{
					NodeID: int32(vv.id),
				}

				for k, vvv := range vv.store {
					n.Stores = append(n.Stores, &sproto.KvnodeStoreStatus{
						StoreID:  int32(k),
						Type:     int32(vvv.Type),
						Value:    int32(vvv.Value),
						IsLeader: vvv.isLeader(),
					})
				}

				s.Nodes = append(s.Nodes, n)
			}

			for _, vv := range v.stores {
				s.Stores = append(s.Stores, &sproto.StoreStatus{
					StoreID: int32(vv.id),
					Slots:   vv.slots.ToJson(),
				})
			}

			resp.Sets = append(resp.Sets, s)
		}

		p.udp.SendTo(from, snet.MakeMessage(m.Context, resp))
	}
}

func (p *pd) onStoreReportStatus(from *net.UDPAddr, m *snet.Message) {
	msg := m.Msg.(*sproto.StoreReportStatus)
	set := p.pState.deployment.sets[int(msg.SetID)]
	if nil == set {
		return
	}
	node := set.nodes[int(msg.NodeID)]
	if nil == node {
		return
	}
	store := node.store[int(msg.StoreID)]
	if nil == store {
		return
	}

	GetSugar().Debugf("onStoreReportStatus node:%d store:%d isLeader:%v", msg.NodeID, msg.StoreID, msg.Isleader)

	store.lastReport = time.Now()
	store.isLead = msg.Isleader
	store.kvcount = int(msg.Kvcount)
	store.progress = msg.Progress
}

func (p *pd) initMsgHandler() {
	p.registerMsgHandler(&sproto.InstallDeployment{}, p.onInstallDeployment)
	p.registerMsgHandler(&sproto.AddSet{}, p.onAddSet)
	p.registerMsgHandler(&sproto.RemSet{}, p.onRemSet)
	p.registerMsgHandler(&sproto.SetMarkClear{}, p.onSetMarkClear)
	p.registerMsgHandler(&sproto.AddNode{}, p.onAddNode)
	p.registerMsgHandler(&sproto.RemNode{}, p.onRemNode)
	p.registerMsgHandler(&sproto.AddLearnerStoreToNode{}, p.onAddLearnerStoreToNode)
	p.registerMsgHandler(&sproto.PromoteLearnerStore{}, p.onPromoteLearnerStore)
	p.registerMsgHandler(&sproto.RemoveNodeStore{}, p.onRemoveNodeStore)
	p.registerMsgHandler(&sproto.IsTransInReadyResp{}, p.onSlotTransInReady)
	p.registerMsgHandler(&sproto.SlotTransOutOk{}, p.onSlotTransOutOk)
	p.registerMsgHandler(&sproto.SlotTransInOk{}, p.onSlotTransInOk)
	p.registerMsgHandler(&sproto.KvnodeBoot{}, p.onKvnodeBoot)
	p.registerMsgHandler(&sproto.QueryRouteInfo{}, p.onQueryRouteInfo)
	p.registerMsgHandler(&sproto.GetFlyGateList{}, p.onGetFlyGateList)
	p.registerMsgHandler(&sproto.FlyGateHeartBeat{}, p.onFlyGateHeartBeat)
	p.registerMsgHandler(&sproto.ChangeFlyGate{}, p.changeFlyGate)
	p.registerMsgHandler(&sproto.GetMeta{}, p.onGetMeta)
	p.registerMsgHandler(&sproto.SetMeta{}, p.onSetMeta)
	p.registerMsgHandler(&sproto.UpdateMeta{}, p.onUpdateMeta)
	p.registerMsgHandler(&sproto.StoreUpdateMetaOk{}, p.onStoreUpdateMetaOk)
	p.registerMsgHandler(&sproto.GetSetStatus{}, p.onGetSetStatus)
	p.registerMsgHandler(&sproto.StoreReportStatus{}, p.onStoreReportStatus)
}
