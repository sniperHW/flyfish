package flypd

import (
	"fmt"
	"github.com/gogo/protobuf/proto"
	"github.com/sniperHW/flyfish/pkg/etcd/pkg/types"
	"github.com/sniperHW/flyfish/pkg/etcd/raft/raftpb"
	flynet "github.com/sniperHW/flyfish/pkg/net"
	"github.com/sniperHW/flyfish/pkg/raft"
	snet "github.com/sniperHW/flyfish/server/net"
	sproto "github.com/sniperHW/flyfish/server/proto"
	"io/ioutil"
	"math"
	"net"
	"net/http"
	"reflect"
	"strings"
	"time"
)

func (p *pd) registerMsgHandler(msg proto.Message, httpCmd string, handler func(replyer, *snet.Message)) {
	if nil != msg {
		reflectType := reflect.TypeOf(msg)
		p.msgHandler.handles[reflectType] = msgHandle{
			h:            handler,
			isConsoleMsg: "" != httpCmd, //只有控制台命令才注册了http接口
		}
		if "" != httpCmd {
			p.msgHandler.makeHttpReq[httpCmd] = func(r *http.Request) (*snet.Message, error) {
				v, err := ioutil.ReadAll(r.Body)
				if nil != err {
					return nil, err
				}
				req := reflect.New(reflectType.Elem()).Interface().(proto.Message)
				if err = proto.Unmarshal(v, req); nil != err {
					return nil, err
				} else {
					return snet.MakeMessage(0, req), nil
				}
			}
		}
	}
}

func (p *pd) onMsg(replyer replyer, msg *snet.Message) {
	if h, ok := p.msgHandler.handles[reflect.TypeOf(msg.Msg)]; ok {
		if p.config.DisableUdpConsole && h.isConsoleMsg {
			//禁止udp console接口，如果请求来自udp全部
			if _, ok := replyer.(udpReplyer); ok {
				return
			}
		}
		h.h(replyer, msg)
	}
}

func (p *pd) onKvnodeBoot(replyer replyer, m *snet.Message) {
	if 0 == p.pState.Meta.Version {
		GetSugar().Infof("Meta not set")
		//meta尚未初始化
		return
	} else {
		msg := m.Msg.(*sproto.KvnodeBoot)
		node := p.getNode(msg.NodeID)
		if nil == node {
			replyer.reply(snet.MakeMessage(m.Context,
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
			Meta:        p.pState.MetaBytes,
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

		replyer.reply(snet.MakeMessage(m.Context, resp))
	}
}

func (p *pd) onQueryRouteInfo(replyer replyer, m *snet.Message) {
	msg := m.Msg.(*sproto.QueryRouteInfo)
	resp := p.pState.deployment.queryRouteInfo(msg)
	replyer.reply(snet.MakeMessage(m.Context, resp))
}

func (p *pd) onGetFlyGateList(replyer replyer, m *snet.Message) {
	resp := &sproto.GetFlyGateListResp{}
	for _, v := range p.flygateMgr.flygateMap {
		resp.List = append(resp.List, &sproto.Flygate{
			Service:      v.service,
			MsgPerSecond: int32(v.msgPerSecond),
		})
	}
	replyer.reply(snet.MakeMessage(m.Context, resp))
}

func (p *pd) onFlyGateHeartBeat(replyer replyer, m *snet.Message) {
	msg := m.Msg.(*sproto.FlyGateHeartBeat)
	p.flygateMgr.onHeartBeat(msg.GateService, int(msg.MsgPerSecond))
}

func (p *pd) changeFlyGate(replyer replyer, m *snet.Message) {
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
		replyer.reply(snet.MakeMessage(m.Context, &sproto.ChangeFlyGateResp{Ok: true, Service: target.service}))
	} else {
		replyer.reply(snet.MakeMessage(m.Context, &sproto.ChangeFlyGateResp{Ok: false}))
	}
}

func (p *pd) onGetSetStatus(replyer replyer, m *snet.Message) {
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

	replyer.reply(snet.MakeMessage(m.Context, resp))
}

func (p *pd) onStoreReportStatus(_ replyer, m *snet.Message) {
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

	if msg.Isleader && msg.MetaVersion != p.pState.Meta.Version {
		addr, _ := net.ResolveUDPAddr("udp", fmt.Sprintf("%s:%d", node.host, node.servicePort))
		p.udp.SendTo(addr, snet.MakeMessage(0,
			&sproto.NotifyUpdateMeta{
				Store:   int32(msg.StoreID),
				Version: p.pState.Meta.Version,
				Meta:    p.pState.MetaBytes,
			}))
	}
}

func (p *pd) startUdpService() error {
	udp, err := flynet.NewUdp(p.service, snet.Pack, snet.Unpack)
	if nil != err {
		return err
	}

	GetSugar().Infof("flypd start udp at %s", p.service)

	p.udp = udp

	go func() {
		recvbuff := make([]byte, 64*1024)
		for {
			from, msg, err := udp.ReadFrom(recvbuff)
			if nil != err {
				GetSugar().Errorf("read err:%v", err)
				return
			} else {
				p.mainque.append(func() {
					if _, ok := msg.(*snet.Message).Msg.(*sproto.QueryPdLeader); ok {
						p.udp.SendTo(from, snet.MakeMessage(msg.(*snet.Message).Context, &sproto.QueryPdLeaderResp{
							Yes:     p.isLeader(),
							Service: p.service,
						}))
					} else if p.isLeader() {
						p.onMsg(udpReplyer{from: from, pd: p}, msg.(*snet.Message))
					}
				})
			}
		}
	}()

	return nil
}

type ProposalConfChange struct {
	confChangeType raftpb.ConfChangeType
	url            string //for add
	nodeID         uint64
	reply          func(error)
}

func (this *ProposalConfChange) GetType() raftpb.ConfChangeType {
	return this.confChangeType
}

func (this *ProposalConfChange) GetURL() string {
	return this.url
}

func (this *ProposalConfChange) GetNodeID() uint64 {
	return this.nodeID
}

func (this *ProposalConfChange) IsPromote() bool {
	return false
}

func (this *ProposalConfChange) OnError(err error) {
	this.reply(err)
}

func (p *pd) onAddPdNode(replyer replyer, m *snet.Message) {
	msg := m.Msg.(*sproto.AddPdNode)

	reply := func(err error) {

		resp := &sproto.AddPdNodeResp{
			Ok: nil == err,
		}

		if nil != err {
			resp.Reason = err.Error()
		}

		replyer.reply(snet.MakeMessage(m.Context, resp))
	}

	id := raft.MakeInstanceID(uint16(msg.Id), uint16(0))

	if err := p.rn.MayAddMember(types.ID(id)); nil == err {
		p.rn.IssueConfChange(&ProposalConfChange{
			confChangeType: raftpb.ConfChangeAddNode,
			nodeID:         uint64(id),
			url:            msg.Url,
			reply:          reply,
		})
	} else {
		reply(err)
	}
}

func (p *pd) onRemovePdNode(replyer replyer, m *snet.Message) {
	msg := m.Msg.(*sproto.RemovePdNode)

	reply := func(err error) {

		resp := &sproto.RemovePdNodeResp{
			Ok: nil == err,
		}

		if nil != err {
			resp.Reason = err.Error()
		}

		replyer.reply(snet.MakeMessage(m.Context, resp))
	}

	id := raft.MakeInstanceID(uint16(msg.Id), uint16(0))

	if err := p.rn.MayRemoveMember(types.ID(id)); nil == err {
		p.rn.IssueConfChange(&ProposalConfChange{
			confChangeType: raftpb.ConfChangeRemoveNode,
			nodeID:         uint64(id),
			reply:          reply,
		})
	} else {
		reply(err)
	}
}

func (p *pd) initMsgHandler() {
	//for console
	p.registerMsgHandler(&sproto.AddSet{}, "AddSet", p.onAddSet)
	p.registerMsgHandler(&sproto.RemSet{}, "RemSet", p.onRemSet)
	p.registerMsgHandler(&sproto.SetMarkClear{}, "SetMarkClear", p.onSetMarkClear)
	p.registerMsgHandler(&sproto.AddNode{}, "AddNode", p.onAddNode)
	p.registerMsgHandler(&sproto.RemNode{}, "RemNode", p.onRemNode)
	p.registerMsgHandler(&sproto.AddLearnerStoreToNode{}, "AddLearnerStoreToNode", p.onAddLearnerStoreToNode)
	p.registerMsgHandler(&sproto.PromoteLearnerStore{}, "PromoteLearnerStore", p.onPromoteLearnerStore)
	p.registerMsgHandler(&sproto.RemoveNodeStore{}, "RemoveNodeStore", p.onRemoveNodeStore)
	p.registerMsgHandler(&sproto.GetMeta{}, "GetMeta", p.onGetMeta)
	p.registerMsgHandler(&sproto.GetSetStatus{}, "GetSetStatus", p.onGetSetStatus)
	p.registerMsgHandler(&sproto.MetaAddTable{}, "MetaAddTable", p.onUpdateMetaReq)
	p.registerMsgHandler(&sproto.MetaAddFields{}, "MetaAddFields", p.onUpdateMetaReq)
	p.registerMsgHandler(&sproto.MetaRemoveTable{}, "MetaRemoveTable", p.onUpdateMetaReq)
	p.registerMsgHandler(&sproto.MetaRemoveFields{}, "MetaRemoveFields", p.onUpdateMetaReq)
	p.registerMsgHandler(&sproto.AddPdNode{}, "AddPdNode", p.onAddPdNode)
	p.registerMsgHandler(&sproto.RemovePdNode{}, "RemovePdNode", p.onRemovePdNode)

	//servers
	p.registerMsgHandler(&sproto.IsTransInReadyResp{}, "", p.onSlotTransInReady)
	p.registerMsgHandler(&sproto.SlotTransOutOk{}, "", p.onSlotTransOutOk)
	p.registerMsgHandler(&sproto.SlotTransInOk{}, "", p.onSlotTransInOk)
	p.registerMsgHandler(&sproto.KvnodeBoot{}, "", p.onKvnodeBoot)
	p.registerMsgHandler(&sproto.QueryRouteInfo{}, "", p.onQueryRouteInfo)
	p.registerMsgHandler(&sproto.GetFlyGateList{}, "", p.onGetFlyGateList)
	p.registerMsgHandler(&sproto.FlyGateHeartBeat{}, "", p.onFlyGateHeartBeat)
	p.registerMsgHandler(&sproto.ChangeFlyGate{}, "", p.changeFlyGate)
	p.registerMsgHandler(&sproto.StoreReportStatus{}, "", p.onStoreReportStatus)
	p.registerMsgHandler(&sproto.GetScanTableMeta{}, "", p.onGetScanTableMeta)

}
