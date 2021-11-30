package flypd

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/sniperHW/flyfish/pkg/bitmap"
	"github.com/sniperHW/flyfish/pkg/buffer"
	sproto "github.com/sniperHW/flyfish/server/proto"
	"github.com/sniperHW/flyfish/server/slot"
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

func (d deployment) toJson() ([]byte, error) {
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

	return json.Marshal(&deploymentJson)
}

func (d *deployment) loadFromJson(jsonBytes []byte) error {
	d.sets = map[int]*set{}

	var deploymentJson DeploymentJson
	var err error
	if err = json.Unmarshal(jsonBytes, &deploymentJson); err != nil {
		return err
	}

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
