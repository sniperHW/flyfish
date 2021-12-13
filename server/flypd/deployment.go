package flypd

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/sniperHW/flyfish/pkg/bitmap"
	"github.com/sniperHW/flyfish/pkg/buffer"
	snet "github.com/sniperHW/flyfish/server/net"
	sproto "github.com/sniperHW/flyfish/server/proto"
	"github.com/sniperHW/flyfish/server/slot"
	"net"
	"sort"
)

var StorePerSet int = 5          //每个set含有多少个store
var KvNodePerSet int = 1         //每个set含有多少kvnode
var CurrentTransferCount int = 5 //最大并发transfer的slot数量

type KvNodeJson struct {
	NodeID      int
	Host        string
	ServicePort int
	RaftPort    int
}

type StoreJson struct {
	StoreID int
	Slots   []byte
}

type SetJson struct {
	Version   int64
	SetID     int
	KvNodes   []KvNodeJson
	Stores    []StoreJson
	MarkClear bool //需要将其上slot全部移走
}

type DeploymentJson struct {
	Version int64
	Sets    []SetJson
}

type kvnode struct {
	id          int
	host        string
	servicePort int
	raftPort    int
	set         *set
}

type store struct {
	id           int
	slots        *bitmap.Bitmap
	set          *set
	SlotOutCount int //待迁出的slot数量
	SlotInCount  int //待迁入的slot数量
}

type set struct {
	version      int64
	id           int
	markClear    bool
	nodes        map[int]*kvnode
	stores       map[int]*store
	SlotOutCount int //待迁出的slot数量
	SlotInCount  int //待迁入的slot数量
}

func (s *set) getTotalSlotCount() int {
	totalSlotCount := 0
	for _, v := range s.stores {
		totalSlotCount += len(v.slots.GetOpenBits())
	}
	return totalSlotCount
}

type deployment struct {
	version int64
	sets    map[int]*set
}

func (d deployment) getStoreByID(id int) *store {
	for _, v := range d.sets {
		if s, ok := v.stores[id]; ok {
			return s
		}
	}
	return nil
}

func (d deployment) queryRouteInfo(req *sproto.QueryRouteInfo) *sproto.QueryRouteInfoResp {
	resp := &sproto.QueryRouteInfoResp{}
	resp.Version = d.version
	if req.Version >= d.version {
		//路由信息没有发生过变更
	} else {
		var localSets []int32
		for _, v := range d.sets {
			localSets = append(localSets, int32(v.id))
			if v.version > req.Version {
				s := &sproto.RouteInfoSet{
					SetID: int32(v.id),
				}

				for _, vv := range v.nodes {
					s.Kvnodes = append(s.Kvnodes, &sproto.RouteInfoKvNode{
						NodeID:      int32(vv.id),
						Host:        vv.host,
						ServicePort: int32(vv.servicePort),
					})
				}

				for _, vv := range v.stores {
					s.Stores = append(s.Stores, int32(vv.id))
					s.Slots = append(s.Slots, vv.slots.ToJson())
				}
				resp.Sets = append(resp.Sets, s)
			}
		}

		if len(localSets) > 0 && len(req.Sets) > 0 {
			sort.Slice(localSets, func(i, j int) bool {
				return localSets[i] < localSets[j]
			})

			sort.Slice(req.Sets, func(i, j int) bool {
				return req.Sets[i] < req.Sets[j]
			})

			i := 0
			j := 0

			for i < len(localSets) && j < len(req.Sets) {
				if localSets[i] == req.Sets[j] {
					i++
					j++
				} else if localSets[i] > req.Sets[j] {
					resp.RemoveSets = append(resp.RemoveSets, req.Sets[j])
					j++
				} else {
					i++
				}
			}

			if len(req.Sets[j:]) > 0 {
				resp.RemoveSets = append(resp.RemoveSets, req.Sets[j:]...)
			}

		}
	}

	return resp
}

func (d deployment) toDeploymentJson() DeploymentJson {
	var deploymentJson DeploymentJson
	deploymentJson.Version = d.version
	for _, v := range d.sets {
		setJson := SetJson{
			Version:   v.version,
			SetID:     v.id,
			MarkClear: v.markClear,
		}

		for _, vv := range v.nodes {
			setJson.KvNodes = append(setJson.KvNodes, KvNodeJson{
				NodeID:      vv.id,
				Host:        vv.host,
				ServicePort: vv.servicePort,
				RaftPort:    vv.raftPort,
			})
		}

		for _, vv := range v.stores {
			setJson.Stores = append(setJson.Stores, StoreJson{
				StoreID: vv.id,
				Slots:   vv.slots.ToJson(),
			})
		}

		deploymentJson.Sets = append(deploymentJson.Sets, setJson)
	}

	return deploymentJson
}

func (d deployment) toJson() ([]byte, error) {
	deploymentJson := d.toDeploymentJson()
	return json.Marshal(&deploymentJson)
}

func (d *deployment) loadFromDeploymentJson(deploymentJson *DeploymentJson) error {
	var err error
	d.sets = map[int]*set{}
	d.version = deploymentJson.Version
	for _, v := range deploymentJson.Sets {
		s := &set{
			version:   v.Version,
			id:        v.SetID,
			markClear: v.MarkClear,
			nodes:     map[int]*kvnode{},
			stores:    map[int]*store{},
		}

		for _, vv := range v.KvNodes {
			n := &kvnode{
				id:          vv.NodeID,
				host:        vv.Host,
				servicePort: vv.ServicePort,
				raftPort:    vv.RaftPort,
				set:         s,
			}
			s.nodes[vv.NodeID] = n
		}

		for _, vv := range v.Stores {
			st := &store{
				id:  vv.StoreID,
				set: s,
			}
			st.slots, err = bitmap.CreateFromJson(vv.Slots)
			if nil != err {
				return err
			}
			s.stores[vv.StoreID] = st
		}

		d.sets[v.SetID] = s
	}

	return nil

}

func (d *deployment) loadFromJson(jsonBytes []byte) error {
	var deploymentJson DeploymentJson
	var err error
	if err = json.Unmarshal(jsonBytes, &deploymentJson); err != nil {
		return err
	} else {
		return d.loadFromDeploymentJson(&deploymentJson)
	}
}

func (d *deployment) loadFromPB(sets []*sproto.DeploymentSet) error {
	d.sets = map[int]*set{}
	d.version = 1

	nodes := map[int32]bool{}
	services := map[string]bool{}
	rafts := map[string]bool{}

	if len(sets) == 0 {
		return errors.New("empty sets")
	}

	storeCount := len(sets) * StorePerSet
	var storeBitmaps []*bitmap.Bitmap

	for i := 0; i < storeCount; i++ {
		storeBitmaps = append(storeBitmaps, bitmap.New(slot.SlotCount))
	}

	jj := 0
	for i := 0; i < slot.SlotCount; i++ {
		storeBitmaps[jj].Set(i)
		jj = (jj + 1) % storeCount
	}

	for i, v := range sets {
		if _, ok := d.sets[int(v.SetID)]; ok {
			return fmt.Errorf("duplicate set:%d", v.SetID)
		}

		if len(v.Nodes) != KvNodePerSet {
			return fmt.Errorf("node count of set should be %d", KvNodePerSet)
		}

		s := &set{
			version: 1,
			id:      int(v.SetID),
			nodes:   map[int]*kvnode{},
			stores:  map[int]*store{},
		}

		for _, vv := range v.Nodes {
			if _, ok := nodes[vv.NodeID]; ok {
				return fmt.Errorf("duplicate node:%d", vv.NodeID)
			}

			service := fmt.Sprintf("%s:%d", vv.Host, vv.ServicePort)

			if _, ok := services[service]; ok {
				return fmt.Errorf("duplicate service:%s", service)
			}

			raft := fmt.Sprintf("%s:%d", vv.Host, vv.RaftPort)

			if _, ok := rafts[raft]; ok {
				return fmt.Errorf("duplicate inter:%s", raft)
			}

			nodes[vv.NodeID] = true
			services[service] = true
			rafts[raft] = true

			n := &kvnode{
				id:          int(vv.NodeID),
				host:        vv.Host,
				servicePort: int(vv.ServicePort),
				raftPort:    int(vv.RaftPort),
				set:         s,
			}
			s.nodes[int(vv.NodeID)] = n
		}

		for j := 0; j < StorePerSet; j++ {
			st := &store{
				id:    i + j + 1,
				slots: storeBitmaps[i+j],
				set:   s,
			}

			s.stores[st.id] = st
		}

		d.sets[int(v.SetID)] = s
	}

	return nil
}

type ProposalInstallDeployment struct {
	*proposalBase
	d *deployment
}

func (p *ProposalInstallDeployment) Serilize(b []byte) []byte {
	b = buffer.AppendByte(b, byte(proposalInstallDeployment))
	bb, err := p.d.toJson()
	if nil != err {
		panic(err)
	}
	return buffer.AppendBytes(b, bb)
}

func (p *ProposalInstallDeployment) apply() {
	p.pd.deployment = p.d
	p.reply()
}

func (p *pd) replayInstallDeployment(reader *buffer.BufferReader) error {
	d := &deployment{}
	if err := d.loadFromJson(reader.GetAll()); nil != err {
		return err
	}
	p.deployment = d
	return nil
}

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

	for _, v := range p.pd.deployment.sets {
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

	p.pd.deployment.version++
	s.version = p.pd.deployment.version
	p.pd.deployment.sets[s.id] = s

}

func (p *ProposalAddSet) apply() {
	p.doApply()
	p.reply()
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
	delete(p.pd.deployment.sets, p.setID)
	p.pd.deployment.version++
	p.reply()
}

func (p *pd) replayRemSet(reader *buffer.BufferReader) error {
	setID := int(reader.GetInt32())
	delete(p.deployment.sets, setID)
	p.deployment.version++
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
	if set, ok := p.pd.deployment.sets[p.setID]; ok && !set.markClear {
		set.markClear = true
		p.pd.markClearSet[p.setID] = set
		p.pd.slotBalance()
	}
	p.reply()
}

func (p *pd) replaySetMarkClear(reader *buffer.BufferReader) error {
	setID := int(reader.GetInt32())
	if set, ok := p.deployment.sets[setID]; ok && !set.markClear {
		set.markClear = true
		p.markClearSet[setID] = set
	}
	return nil
}

func (p *pd) onInstallDeployment(from *net.UDPAddr, m *snet.Message) {

	msg := m.Msg.(*sproto.InstallDeployment)

	if nil != p.pState.MetaTransaction {
		p.udp.SendTo(from, snet.MakeMessage(m.Context,
			&sproto.InstallDeploymentResp{
				Ok:     false,
				Reason: "wait for previous meta transaction finish",
			}))
		return
	}

	if nil != p.deployment {
		p.udp.SendTo(from, snet.MakeMessage(m.Context,
			&sproto.InstallDeploymentResp{
				Ok:     false,
				Reason: "already install",
			}))
		return
	}

	d := &deployment{}
	if err := d.loadFromPB(msg.Sets); nil != err {
		p.udp.SendTo(from, snet.MakeMessage(m.Context,
			&sproto.InstallDeploymentResp{
				Ok:     false,
				Reason: err.Error(),
			}))
		return
	}

	err := p.issueProposal(&ProposalInstallDeployment{
		d: d,
		proposalBase: &proposalBase{
			pd: p,
			reply: func(err ...error) {
				if len(err) == 0 {
					p.udp.SendTo(from, snet.MakeMessage(m.Context,
						&sproto.InstallDeploymentResp{
							Ok: true,
						}))
				} else {
					p.udp.SendTo(from, snet.MakeMessage(m.Context,
						&sproto.InstallDeploymentResp{
							Ok:     false,
							Reason: err[0].Error(),
						}))
				}
			},
		},
	})

	if nil != err {
		p.udp.SendTo(from, snet.MakeMessage(m.Context,
			&sproto.InstallDeploymentResp{
				Ok:     false,
				Reason: err.Error(),
			}))
	}

}

func (p *pd) onRemSet(from *net.UDPAddr, m *snet.Message) {
	msg := m.Msg.(*sproto.RemSet)

	if nil != p.pState.MetaTransaction {
		p.udp.SendTo(from, snet.MakeMessage(m.Context,
			&sproto.RemSetResp{
				Ok:     false,
				Reason: "wait for previous meta transaction finish",
			}))
		return
	}

	if nil == p.deployment {
		p.udp.SendTo(from, snet.MakeMessage(m.Context,
			&sproto.RemSetResp{
				Ok:     false,
				Reason: "no deployment",
			}))
		return
	}

	s, ok := p.deployment.sets[int(msg.SetID)]
	if !ok {
		p.udp.SendTo(from, snet.MakeMessage(m.Context,
			&sproto.RemSetResp{
				Ok:     false,
				Reason: "set not exists",
			}))
		return
	}

	//只有当s中所有的store都不存在slot时才能移除
	for _, v := range s.stores {
		if len(v.slots.GetOpenBits()) != 0 {
			p.udp.SendTo(from, snet.MakeMessage(m.Context,
				&sproto.RemSetResp{
					Ok:     false,
					Reason: fmt.Sprintf("there are slots in store:%d", v.id),
				}))
			return
		}
	}

	err := p.issueProposal(&ProposalRemSet{
		setID: int(msg.SetID),
		proposalBase: &proposalBase{
			pd: p,
			reply: func(err ...error) {
				if len(err) == 0 {
					p.udp.SendTo(from, snet.MakeMessage(m.Context,
						&sproto.RemSetResp{
							Ok: true,
						}))
				} else {
					p.udp.SendTo(from, snet.MakeMessage(m.Context,
						&sproto.RemSetResp{
							Ok:     false,
							Reason: err[0].Error(),
						}))
				}
			},
		},
	})

	if nil != err {
		p.udp.SendTo(from, snet.MakeMessage(m.Context,
			&sproto.RemSetResp{
				Ok:     false,
				Reason: err.Error(),
			}))
	}

}

func (p *pd) onSetMarkClear(from *net.UDPAddr, m *snet.Message) {
	msg := m.Msg.(*sproto.SetMarkClear)
	if nil == p.deployment {
		p.udp.SendTo(from, snet.MakeMessage(m.Context,
			&sproto.SetMarkClearResp{
				Ok:     false,
				Reason: "no deployment",
			}))
		return
	}

	s, ok := p.deployment.sets[int(msg.SetID)]
	if !ok {
		p.udp.SendTo(from, snet.MakeMessage(m.Context,
			&sproto.SetMarkClearResp{
				Ok:     false,
				Reason: "set not exists",
			}))
		return
	}

	if s.markClear {
		p.udp.SendTo(from, snet.MakeMessage(m.Context,
			&sproto.SetMarkClearResp{
				Ok: true,
			}))
		return
	}

	err := p.issueProposal(&ProposalSetMarkClear{
		setID: int(msg.SetID),
		proposalBase: &proposalBase{
			pd: p,
			reply: func(err ...error) {
				if len(err) == 0 {
					p.udp.SendTo(from, snet.MakeMessage(m.Context,
						&sproto.SetMarkClearResp{
							Ok: true,
						}))
				} else {
					p.udp.SendTo(from, snet.MakeMessage(m.Context,
						&sproto.SetMarkClearResp{
							Ok:     false,
							Reason: err[0].Error(),
						}))
				}
			},
		},
	})

	if nil != err {
		p.udp.SendTo(from, snet.MakeMessage(m.Context,
			&sproto.SetMarkClearResp{
				Ok:     false,
				Reason: err.Error(),
			}))
	}

}

func (p *pd) onAddSet(from *net.UDPAddr, m *snet.Message) {
	msg := m.Msg.(*sproto.AddSet)

	if nil != p.pState.MetaTransaction {
		p.udp.SendTo(from, snet.MakeMessage(m.Context,
			&sproto.AddSetResp{
				Ok:     false,
				Reason: "wait for previous meta transaction finish",
			}))
		return
	}

	if nil == p.deployment {
		p.udp.SendTo(from, snet.MakeMessage(m.Context,
			&sproto.AddSetResp{
				Ok:     false,
				Reason: "no deployment",
			}))
		return
	}

	_, ok := p.deployment.sets[int(msg.Set.SetID)]
	if ok {
		p.udp.SendTo(from, snet.MakeMessage(m.Context,
			&sproto.AddSetResp{
				Ok:     false,
				Reason: "set already exists",
			}))
		return
	}

	//检查node是否有冲突
	nodeIDS := map[int]bool{}
	nodeServices := map[string]bool{}
	nodeRafts := map[string]bool{}

	for _, v := range p.deployment.sets {
		for _, vv := range v.nodes {
			nodeIDS[vv.id] = true
			nodeServices[fmt.Sprintf("%s:%d", vv.host, vv.servicePort)] = true
			nodeRafts[fmt.Sprintf("%s:%d", vv.host, vv.raftPort)] = true
		}
	}

	for _, v := range msg.Set.Nodes {
		if nodeIDS[int(v.NodeID)] {
			p.udp.SendTo(from, snet.MakeMessage(m.Context,
				&sproto.AddSetResp{
					Ok:     false,
					Reason: fmt.Sprintf("duplicate node:%d", v.NodeID),
				}))
			return
		}

		if nodeServices[fmt.Sprintf("%s:%d", v.Host, v.ServicePort)] {
			p.udp.SendTo(from, snet.MakeMessage(m.Context,
				&sproto.AddSetResp{
					Ok:     false,
					Reason: fmt.Sprintf("duplicate service:%s:%d", v.Host, v.ServicePort),
				}))
			return
		}

		if nodeRafts[fmt.Sprintf("%s:%d", v.Host, v.RaftPort)] {
			p.udp.SendTo(from, &sproto.AddSetResp{
				Ok:     false,
				Reason: fmt.Sprintf("duplicate inter:%s:%d", v.Host, v.RaftPort),
			})
			return
		}

		nodeIDS[int(v.NodeID)] = true
		nodeServices[fmt.Sprintf("%s:%d", v.Host, v.ServicePort)] = true
		nodeRafts[fmt.Sprintf("%s:%d", v.Host, v.RaftPort)] = true
	}

	err := p.issueProposal(&ProposalAddSet{
		msg: msg,
		proposalBase: &proposalBase{
			pd: p,
			reply: func(err ...error) {
				if len(err) == 0 {
					p.udp.SendTo(from, snet.MakeMessage(m.Context,
						&sproto.AddSetResp{
							Ok: true,
						}))
				} else {
					p.udp.SendTo(from, snet.MakeMessage(m.Context,
						&sproto.AddSetResp{
							Ok:     false,
							Reason: err[0].Error(),
						}))
				}
			},
		},
	})

	if nil != err {
		p.udp.SendTo(from, snet.MakeMessage(m.Context,
			&sproto.AddSetResp{
				Ok:     false,
				Reason: err.Error(),
			}))
	}

}

func (p *pd) onAddNode(from *net.UDPAddr, m *snet.Message) {
	msg := m.Msg.(*sproto.AddNode)
	if nil == p.deployment {
		p.udp.SendTo(from, snet.MakeMessage(m.Context,
			&sproto.AddNodeResp{
				Ok:     false,
				Reason: "no deployment",
			}))
		return
	}

	_, ok := p.pState.AddingNode[int(msg.NodeID)]
	if ok {
		p.udp.SendTo(from, snet.MakeMessage(m.Context,
			&sproto.AddNodeResp{
				Ok: true,
			}))
		return
	}

	s, ok := p.deployment.sets[int(msg.SetID)]
	if !ok {
		p.udp.SendTo(from, snet.MakeMessage(m.Context,
			&sproto.AddNodeResp{
				Ok:     false,
				Reason: "set not found",
			}))
		return
	}

	_, ok = s.nodes[int(msg.NodeID)]
	if ok {
		p.udp.SendTo(from, snet.MakeMessage(m.Context,
			&sproto.AddNodeResp{
				Ok:     false,
				Reason: "duplicate node id",
			}))
		return
	}

	//检查是否存在重复服务地址
	for _, v := range p.deployment.sets {
		for _, vv := range v.nodes {
			if vv.host == msg.Host && vv.servicePort == int(msg.ServicePort) {
				p.udp.SendTo(from, snet.MakeMessage(m.Context,
					&sproto.AddNodeResp{
						Ok:     false,
						Reason: "duplicate service addr",
					}))
				return
			}

			if vv.host == msg.Host && vv.raftPort == int(msg.RaftPort) {
				p.udp.SendTo(from, snet.MakeMessage(m.Context,
					&sproto.AddNodeResp{
						Ok:     false,
						Reason: "duplicate inter addr",
					}))
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
					p.udp.SendTo(from, snet.MakeMessage(m.Context,
						&sproto.AddNodeResp{
							Ok: true,
						}))
				} else {
					p.udp.SendTo(from, snet.MakeMessage(m.Context,
						&sproto.AddNodeResp{
							Ok:     false,
							Reason: err[0].Error(),
						}))
				}
			},
		},
	})

	if nil != err {
		p.udp.SendTo(from, snet.MakeMessage(m.Context,
			&sproto.AddNodeResp{
				Ok:     false,
				Reason: err.Error(),
			}))
	}

}

func (p *pd) onNotifyAddNodeResp(from *net.UDPAddr, m *snet.Message) {

	msg := m.Msg.(*sproto.NotifyAddNodeResp)
	an, ok := p.pState.AddingNode[int(msg.NodeID)]
	if ok && an.context == m.Context {

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

func (p *pd) onRemNode(from *net.UDPAddr, m *snet.Message) {
	msg := m.Msg.(*sproto.RemNode)
	if nil == p.deployment {
		p.udp.SendTo(from, snet.MakeMessage(m.Context,
			&sproto.RemNodeResp{
				Ok:     false,
				Reason: "no deployment",
			}))
		return
	}

	_, ok := p.pState.RemovingNode[int(msg.NodeID)]
	if ok {
		p.udp.SendTo(from, snet.MakeMessage(m.Context,
			&sproto.RemNodeResp{
				Ok: true,
			}))
		return
	}

	s, ok := p.deployment.sets[int(msg.SetID)]
	if !ok {
		p.udp.SendTo(from, snet.MakeMessage(m.Context,
			&sproto.RemNodeResp{
				Ok:     false,
				Reason: "set not found",
			}))
		return
	}

	_, ok = s.nodes[int(msg.NodeID)]
	if !ok {
		p.udp.SendTo(from, snet.MakeMessage(m.Context,
			&sproto.RemNodeResp{
				Ok:     false,
				Reason: "node not found",
			}))
		return
	}

	//不允许将节点数量减少到KvNodePerSet以下
	if len(s.nodes)-1 < KvNodePerSet {
		if !ok {
			p.udp.SendTo(from, snet.MakeMessage(m.Context,
				&sproto.RemNodeResp{
					Ok:     false,
					Reason: fmt.Sprintf("cannot remove node,should keep %d node per set", KvNodePerSet),
				}))
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
					p.udp.SendTo(from, snet.MakeMessage(m.Context,
						&sproto.RemNodeResp{
							Ok: true,
						}))
				} else {
					p.udp.SendTo(from, snet.MakeMessage(m.Context,
						&sproto.RemNodeResp{
							Ok:     false,
							Reason: err[0].Error(),
						}))
				}
			},
		},
	})

	if nil != err {
		p.udp.SendTo(from, snet.MakeMessage(m.Context,
			&sproto.RemNodeResp{
				Ok:     false,
				Reason: err.Error(),
			}))
	}
}

func (p *pd) onNotifyRemNodeResp(from *net.UDPAddr, m *snet.Message) {
	msg := m.Msg.(*sproto.NotifyRemNodeResp)
	rn, ok := p.pState.RemovingNode[int(msg.NodeID)]
	if ok && rn.context == m.Context {
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
