package flypd

import (
	"fmt"
	"github.com/gogo/protobuf/proto"
	"github.com/sniperHW/flyfish/db/sql"
	"github.com/sniperHW/flyfish/pkg/etcd/pkg/types"
	"github.com/sniperHW/flyfish/pkg/etcd/raft/raftpb"
	flynet "github.com/sniperHW/flyfish/pkg/net"
	"github.com/sniperHW/flyfish/pkg/raft/membership"
	snet "github.com/sniperHW/flyfish/server/net"
	sproto "github.com/sniperHW/flyfish/server/proto"
	"io/ioutil"
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

func buildRaftClusterStr(set *Set, storeId int) (raftCluster []string) {
	for _, n := range set.Nodes {
		if v, ok := n.Store[storeId]; ok {
			switch v.StoreType {
			case LearnerStore:
				raftCluster = append(raftCluster, fmt.Sprintf("%d@%d@http://%s:%d@%s:%d@learner", n.NodeID, v.RaftID, n.Host, n.RaftPort, n.Host, n.ServicePort))
			case VoterStore:
				raftCluster = append(raftCluster, fmt.Sprintf("%d@%d@http://%s:%d@%s:%d@voter", n.NodeID, v.RaftID, n.Host, n.RaftPort, n.Host, n.ServicePort))
			case RemovingStore:
				if v.StoreTypeBeforeRemove == LearnerStore {
					raftCluster = append(raftCluster, fmt.Sprintf("%d@%d@http://%s:%d@%s:%d@learner", n.NodeID, v.RaftID, n.Host, n.RaftPort, n.Host, n.ServicePort))
				} else if v.StoreTypeBeforeRemove == VoterStore {
					raftCluster = append(raftCluster, fmt.Sprintf("%d@%d@http://%s:%d@%s:%d@voter", n.NodeID, v.RaftID, n.Host, n.RaftPort, n.Host, n.ServicePort))
				}
			}
		}
	}
	return
}

func (p *pd) onKvnodeBoot(replyer replyer, m *snet.Message) {

	msg := m.Msg.(*sproto.KvnodeBoot)
	set, node := p.getNode(msg.NodeID)
	if nil == node {
		replyer.reply(snet.MakeMessage(m.Context,
			&sproto.KvnodeBootResp{
				Ok:     false,
				Reason: fmt.Sprintf("node:%d not in deployment", msg.NodeID),
			}))
		return
	}

	GetSugar().Infof("onKvnodeBoot node:%d stores:%v", msg.NodeID, node.Store)

	resp := &sproto.KvnodeBootResp{
		Ok:          true,
		ServiceHost: node.Host,
		SetID:       int32(set.SetID),
		ServicePort: int32(node.ServicePort),
		RaftPort:    int32(node.RaftPort),
		Meta:        p.DbMetaMgr.dbMetaBytes,
	}

	for storeId, st := range node.Store {
		if st.StoreType == AddLearnerStore || st.StoreTypeBeforeRemove == AddLearnerStore {
			continue
		}

		if raftCluster := buildRaftClusterStr(set, storeId); len(raftCluster) > 0 {
			GetSugar().Infof("%v", raftCluster)
			s := &sproto.StoreInfo{
				Id:          int32(storeId),
				Slots:       set.Stores[storeId].Slots,
				RaftCluster: strings.Join(raftCluster, ","),
				RaftID:      st.RaftID,
			}
			resp.Stores = append(resp.Stores, s)
		}
	}

	replyer.reply(snet.MakeMessage(m.Context, resp))

}

func (p *pd) onQueryRouteInfo(replyer replyer, m *snet.Message) {
	msg := m.Msg.(*sproto.QueryRouteInfo)
	resp := p.Deployment.queryRouteInfo(msg)
	replyer.reply(snet.MakeMessage(m.Context, resp))
}

func (p *pd) onGetFlyGateList(replyer replyer, m *snet.Message) {
	resp := &sproto.GetFlyGateListResp{}
	for _, v := range p.flygateMgr.flygateMap {
		resp.List = append(resp.List, &sproto.Flygate{
			Service: v.service,
		})
	}

	replyer.reply(snet.MakeMessage(m.Context, resp))
}

func (p *pd) onFlyGateHeartBeat(replyer replyer, m *snet.Message) {
	msg := m.Msg.(*sproto.FlyGateHeartBeat)
	p.flygateMgr.onHeartBeat(p, msg.GateService)
}

func (p *pd) onGetKvStatus(replyer replyer, m *snet.Message) {
	resp := &sproto.GetKvStatusResp{
		Now:               time.Now().Unix(),
		FreeSlotCount:     int32(len(p.SlotTransferMgr.FreeSlots)),
		TransferSlotCount: int32(len(p.SlotTransferMgr.Transactions) + len(p.SlotTransferMgr.Plan)),
	}

	for _, set := range p.Deployment.Sets {
		s := &sproto.SetStatus{
			SetID:     int32(set.SetID),
			MarkClear: set.MarkClear,
		}
		kvcount := map[int]int{}
		metaVersion := map[int]int64{}
		halt := map[int]bool{}
		for _, node := range set.Nodes {
			n := &sproto.KvnodeStatus{
				NodeID:  int32(node.NodeID),
				Service: fmt.Sprintf("%s:%d", node.Host, node.ServicePort),
			}

			for k, store := range node.Store {
				n.Stores = append(n.Stores, &sproto.KvnodeStoreStatus{
					StoreID:     int32(k),
					StoreType:   int32(store.StoreType),
					IsLeader:    store.isLeader(),
					Progress:    store.progress,
					Halt:        store.halt,
					MetaVersion: store.metaVersion,
					RaftID:      store.RaftID,
					LastReport:  store.lastReport.Unix(),
				})

				if store.isLeader() {
					kvcount[k] = store.kvcount
					metaVersion[k] = store.metaVersion
					halt[k] = store.halt
				}
			}

			s.Nodes = append(s.Nodes, n)
		}

		for _, store := range set.Stores {
			st := &sproto.StoreStatus{
				StoreID:     int32(store.StoreID),
				Slotcount:   int32(len(store.slots.GetOpenBits())),
				Kvcount:     int32(kvcount[store.StoreID]),
				Halt:        halt[store.StoreID],
				MetaVersion: metaVersion[store.StoreID],
			}
			s.Stores = append(s.Stores, st)
			s.Kvcount += st.Kvcount
		}
		resp.Kvcount += s.Kvcount
		resp.Sets = append(resp.Sets, s)
	}

	replyer.reply(snet.MakeMessage(m.Context, resp))
}

func (p *pd) onKvnodeReportStatus(replyer replyer, m *snet.Message) {
	msg := m.Msg.(*sproto.KvnodeReportStatus)
	set := p.Deployment.Sets[int(msg.SetID)]
	if nil == set {
		return
	}
	node := set.Nodes[int(msg.NodeID)]
	if nil == node {
		return
	}

	now := time.Now()

	for _, v := range msg.Stores {
		store := node.Store[int(v.StoreID)]
		if nil == store || store.RaftID != v.RaftID {
			return
		}
		GetSugar().Debugf("onKvnodeReportStatus set:%d node:%d store:%d isLeader:%v kvcount:%d", msg.SetID, msg.NodeID, v.StoreID, v.Isleader, v.Kvcount)
		store.lastReport = now
		store.isLead = v.Isleader
		store.kvcount = int(v.Kvcount)
		store.progress = v.Progress
		store.halt = v.Halt
		store.metaVersion = v.MetaVersion

		if v.Isleader && p.DbMetaMgr.DbMeta.Version > v.MetaVersion {
			addr, _ := net.ResolveUDPAddr("udp", fmt.Sprintf("%s:%d", node.Host, node.ServicePort))
			p.udp.SendTo(addr, snet.MakeMessage(0,
				&sproto.NotifyUpdateMeta{
					Store:   int32(v.StoreID),
					Version: p.DbMetaMgr.DbMeta.Version,
					Meta:    p.DbMetaMgr.dbMetaBytes,
				}))
		}
	}

	isMissing := func(storeID int) (missing bool) {
		missing = true
		for _, v := range msg.Stores {
			if int(v.StoreID) == storeID {
				missing = false
				return
			}
		}
		return
	}

	//检查是否有遗漏的store,有的通知kvnode加载
	var notify *sproto.NotifyMissingStores
	for storeId, st := range node.Store {
		if st.StoreType == AddLearnerStore || st.StoreTypeBeforeRemove == AddLearnerStore {
			continue
		}

		if isMissing(storeId) {

			GetSugar().Infof("node:%d missing:%d", msg.NodeID, storeId)

			if nil == notify {
				notify = &sproto.NotifyMissingStores{
					Meta: p.DbMetaMgr.dbMetaBytes,
				}
			}

			if raftCluster := buildRaftClusterStr(set, storeId); len(raftCluster) > 0 {
				s := &sproto.StoreInfo{
					Id:          int32(storeId),
					Slots:       set.Stores[storeId].Slots,
					RaftCluster: strings.Join(raftCluster, ","),
					RaftID:      st.RaftID,
				}
				notify.Stores = append(notify.Stores, s)
			}
		}
	}

	if nil != notify {
		addr, _ := net.ResolveUDPAddr("udp", fmt.Sprintf("%s:%d", node.Host, node.ServicePort))
		p.udp.SendTo(addr, snet.MakeMessage(0, notify))
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
						p.onMsg(&udpReplyer{from: from, pd: p}, msg.(*snet.Message))
					}
				})
			}
		}
	}()

	return nil
}

func (p *pd) onAddPdNode(replyer replyer, m *snet.Message) {
	msg := m.Msg.(*sproto.AddPdNode)

	raftID := p.raftIDGen.Next()

	reply := func(err error) {

		resp := &sproto.AddPdNodeResp{
			Ok: nil == err,
		}

		if nil != err {
			resp.Reason = err.Error()
		}

		if nil == err || err == membership.ErrProcessIDexists {
			resp.RaftID = raftID
			resp.Cluster = uint32(p.cluster)
			resp.RaftCluster = p.rn.GetRaftCluster()
		}

		replyer.reply(snet.MakeMessage(m.Context, resp))
	}

	if err := p.rn.MayAddMember(types.ID(raftID)); nil == err {
		p.rn.IssueConfChange(&ProposalConfChange{
			confChangeType: raftpb.ConfChangeAddNode,
			nodeID:         raftID,
			processID:      uint16(msg.Id),
			url:            msg.Url,
			clientUrl:      msg.ClientUrl,
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

	if err := p.rn.MayRemoveMember(types.ID(msg.RaftID)); nil == err {
		p.rn.IssueConfChange(&ProposalConfChange{
			confChangeType: raftpb.ConfChangeRemoveNode,
			nodeID:         msg.RaftID,
			reply:          reply,
		})
	} else {
		reply(err)
	}

}

func (p *pd) onListPdMembers(replyer replyer, m *snet.Message) {
	resp := &sproto.ListPdMembersResp{}
	for _, v := range p.rn.Members() {
		str := fmt.Sprintf("nodeID:%d raftID:%x raftURL:%s,clientURL:%s", v.ProcessID, v.ID, v.URL, v.ClientURL)
		if v.ID == p.rn.ID() {
			str += " Leader"
		} else {
			str += " Voter"
		}
		resp.Members = append(resp.Members, str)
	}
	replyer.reply(snet.MakeMessage(m.Context, resp))
}

func (p *pd) onClearDBData(replyer replyer, m *snet.Message) {
	resp := &sproto.ClearDBDataResp{Ok: true}
	meta, err := sql.CreateDbMeta(&p.DbMetaMgr.DbMeta)
	if nil == err {
		dbc, err := sql.SqlOpen(p.config.DBConfig.DBType, p.config.DBConfig.Host, p.config.DBConfig.Port, p.config.DBConfig.DB, p.config.DBConfig.User, p.config.DBConfig.Password)
		if nil == err {
			defer dbc.Close()
			msg := m.Msg.(*sproto.ClearDBData)
			if len(msg.Tables) == 0 {
				err = sql.ClearAllTableData(dbc, meta)
			} else {
				for _, v := range msg.Tables {
					if err = sql.ClearTableData(dbc, meta, v); nil != err {
						break
					}
				}
			}
		}
	}

	if nil != err {
		resp.Ok = false
		resp.Reason = err.Error()
	}

	replyer.reply(snet.MakeMessage(m.Context, resp))

}

func (p *pd) onClearCache(replyer replyer, m *snet.Message) {
	for _, set := range p.Deployment.Sets {
		for _, node := range set.Nodes {
			for id, store := range node.Store {
				if store.isLeader() {
					addr, _ := net.ResolveUDPAddr("udp", fmt.Sprintf("%s:%d", node.Host, node.ServicePort))
					p.udp.SendTo(addr, snet.MakeMessage(0, &sproto.ClearStoreCache{
						Store: int32(id),
					}))
				}
			}
		}
	}

	replyer.reply(snet.MakeMessage(m.Context, &sproto.ClearCacheResp{Ok: true}))

}

func (p *pd) onSuspendKvStore(replyer replyer, m *snet.Message) {
	for _, set := range p.Deployment.Sets {
		for _, node := range set.Nodes {
			for storeId, store := range node.Store {
				if store.isLeader() {
					addr, _ := net.ResolveUDPAddr("udp", fmt.Sprintf("%s:%d", node.Host, node.ServicePort))
					p.udp.SendTo(addr, snet.MakeMessage(0, &sproto.SuspendStore{
						Store: int32(storeId),
					}))
				}
			}
		}
	}

	replyer.reply(snet.MakeMessage(m.Context, &sproto.SuspendKvStoreResp{
		Ok: true,
	}))

}

func (p *pd) onResumeKvStore(replyer replyer, m *snet.Message) {
	for _, set := range p.Deployment.Sets {
		for _, node := range set.Nodes {
			for storeId, store := range node.Store {
				if store.isLeader() {
					addr, _ := net.ResolveUDPAddr("udp", fmt.Sprintf("%s:%d", node.Host, node.ServicePort))
					p.udp.SendTo(addr, snet.MakeMessage(0, &sproto.ResumeStore{
						Store: int32(storeId),
					}))
				}
			}
		}
	}

	replyer.reply(snet.MakeMessage(m.Context, &sproto.ResumeKvStoreResp{
		Ok: true,
	}))
}

func (p *pd) onGetDeployment(replyer replyer, m *snet.Message) {
	resp := &sproto.GetDeploymentResp{}
	for _, v := range p.Deployment.Sets {
		s := &sproto.Set{
			Id:        int32(v.SetID),
			MarkClear: v.MarkClear,
		}

		for _, vv := range v.Nodes {
			s.Nodes = append(s.Nodes, &sproto.Node{
				Id:          int32(vv.NodeID),
				Host:        vv.Host,
				ServicePort: int32(vv.ServicePort),
				RaftPort:    int32(vv.RaftPort),
			})
		}

		resp.Sets = append(resp.Sets, s)
	}

	replyer.reply(snet.MakeMessage(m.Context, resp))
}

func (p *pd) initMsgHandler() {
	//for console
	p.registerMsgHandler(&sproto.AddSet{}, "AddSet", p.onAddSet)
	p.registerMsgHandler(&sproto.RemSet{}, "RemSet", p.onRemSet)
	p.registerMsgHandler(&sproto.SetMarkClear{}, "SetMarkClear", p.onSetMarkClear)
	p.registerMsgHandler(&sproto.AddNode{}, "AddNode", p.onAddNode)
	p.registerMsgHandler(&sproto.RemNode{}, "RemNode", p.onRemNode)
	p.registerMsgHandler(&sproto.GetMeta{}, "GetMeta", p.onGetMeta)
	p.registerMsgHandler(&sproto.GetKvStatus{}, "GetKvStatus", p.onGetKvStatus)
	p.registerMsgHandler(&sproto.MetaAddTable{}, "MetaAddTable", p.onUpdateMetaReq)
	p.registerMsgHandler(&sproto.MetaAddFields{}, "MetaAddFields", p.onUpdateMetaReq)
	p.registerMsgHandler(&sproto.MetaRemoveTable{}, "MetaRemoveTable", p.onUpdateMetaReq)
	p.registerMsgHandler(&sproto.MetaRemoveFields{}, "MetaRemoveFields", p.onUpdateMetaReq)
	p.registerMsgHandler(&sproto.AddPdNode{}, "AddPdNode", p.onAddPdNode)
	p.registerMsgHandler(&sproto.RemovePdNode{}, "RemovePdNode", p.onRemovePdNode)
	p.registerMsgHandler(&sproto.ListPdMembers{}, "ListPdMembers", p.onListPdMembers)
	p.registerMsgHandler(&sproto.ClearDBData{}, "ClearDBData", p.onClearDBData)
	p.registerMsgHandler(&sproto.ClearCache{}, "ClearCache", p.onClearCache)
	p.registerMsgHandler(&sproto.SuspendKvStore{}, "SuspendKvStore", p.onSuspendKvStore)
	p.registerMsgHandler(&sproto.ResumeKvStore{}, "ResumeKvStore", p.onResumeKvStore)
	p.registerMsgHandler(&sproto.GetDeployment{}, "GetDeployment", p.onGetDeployment)

	//servers
	p.registerMsgHandler(&sproto.IsTransInReadyResp{}, "", p.onSlotTransInReady)
	p.registerMsgHandler(&sproto.SlotTransOutOk{}, "", p.onSlotTransOutOk)
	p.registerMsgHandler(&sproto.SlotTransInOk{}, "", p.onSlotTransInOk)
	p.registerMsgHandler(&sproto.KvnodeBoot{}, "", p.onKvnodeBoot)
	p.registerMsgHandler(&sproto.QueryRouteInfo{}, "QueryRouteInfo", p.onQueryRouteInfo)
	p.registerMsgHandler(&sproto.GetFlyGateList{}, "", p.onGetFlyGateList)
	p.registerMsgHandler(&sproto.FlyGateHeartBeat{}, "", p.onFlyGateHeartBeat)
	p.registerMsgHandler(&sproto.KvnodeReportStatus{}, "", p.onKvnodeReportStatus)
	p.registerMsgHandler(&sproto.GetScanTableMeta{}, "", p.onGetScanTableMeta)

}
