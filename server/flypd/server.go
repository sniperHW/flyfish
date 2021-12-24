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
		ServicePort: int32(node.servicePort),
		RaftPort:    int32(node.raftPort),
		MetaVersion: p.pState.Meta.Version,
		Meta:        p.pState.Meta.MetaBytes,
	}

	for _, store := range node.set.stores {
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
	p.flygateMgr.onHeartBeat(msg.GateService, msg.Token, int(msg.MsgPerSecond))
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

func (p *pd) initMsgHandler() {
	p.registerMsgHandler(&sproto.InstallDeployment{}, p.onInstallDeployment)
	p.registerMsgHandler(&sproto.AddSet{}, p.onAddSet)
	p.registerMsgHandler(&sproto.RemSet{}, p.onRemSet)
	p.registerMsgHandler(&sproto.SetMarkClear{}, p.onSetMarkClear)
	p.registerMsgHandler(&sproto.AddNode{}, p.onAddNode)
	p.registerMsgHandler(&sproto.RemNode{}, p.onRemNode)
	//p.registerMsgHandler(&sproto.NotifyAddLearnerNodeResp{}, p.onNotifyAddLearnerNodeResp)
	//p.registerMsgHandler(&sproto.NotifyRemNodeResp{}, p.onNotifyRemNodeResp)
	p.registerMsgHandler(&sproto.NotifySlotTransOutResp{}, p.onNotifySlotTransOutResp)
	p.registerMsgHandler(&sproto.NotifySlotTransInResp{}, p.onNotifySlotTransInResp)
	p.registerMsgHandler(&sproto.KvnodeBoot{}, p.onKvnodeBoot)
	p.registerMsgHandler(&sproto.QueryRouteInfo{}, p.onQueryRouteInfo)
	p.registerMsgHandler(&sproto.GetFlyGateList{}, p.onGetFlyGateList)
	p.registerMsgHandler(&sproto.FlyGateHeartBeat{}, p.onFlyGateHeartBeat)
	p.registerMsgHandler(&sproto.ChangeFlyGate{}, p.changeFlyGate)
	p.registerMsgHandler(&sproto.GetMeta{}, p.onGetMeta)
	p.registerMsgHandler(&sproto.SetMeta{}, p.onSetMeta)
	p.registerMsgHandler(&sproto.UpdateMeta{}, p.onUpdateMeta)
	p.registerMsgHandler(&sproto.NotifyUpdateMetaResp{}, p.onNotifyUpdateMetaResp)
}
