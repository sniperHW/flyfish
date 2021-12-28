package flypd

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/gogo/protobuf/proto"
	"github.com/sniperHW/flyfish/pkg/bitmap"
	snet "github.com/sniperHW/flyfish/server/net"
	sproto "github.com/sniperHW/flyfish/server/proto"
	"github.com/sniperHW/flyfish/server/slot"
	"net"
	"reflect"
	"sort"
)

var StorePerSet int = 6          //每个set含有多少个store
var MinReplicaPerSet int = 1     //最少副本数
var CurrentTransferCount int = 6 //最大并发transfer的slot数量

type FlyKvStoreStateType uint16
type FlyKvStoreStateValue uint16

const (
	LearnerStore  = FlyKvStoreStateType(1)
	VoterStore    = FlyKvStoreStateType(2)
	RemoveStore   = FlyKvStoreStateType(3)
	FlyKvCommited = FlyKvStoreStateValue(1)
	FlyKvUnCommit = FlyKvStoreStateValue(2)
)

type FlyKvStoreState struct {
	Type  FlyKvStoreStateType
	Value FlyKvStoreStateValue
}

type KvNodeJson struct {
	NodeID      int
	Host        string
	ServicePort int
	RaftPort    int
	Store       map[int]*FlyKvStoreState
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
	store       map[int]*FlyKvStoreState
}

func (n *kvnode) isVoter(store int) bool {
	s, ok := n.store[store]
	return ok && s.Type == VoterStore
}

func (n *kvnode) isLearner(store int) (yes bool) {
	s, ok := n.store[store]
	return ok && s.Type == LearnerStore
}

type store struct {
	id           int
	slots        *bitmap.Bitmap
	set          *set
	slotOutCount int //待迁出的slot数量
	slotInCount  int //待迁入的slot数量
}

type set struct {
	version      int64
	id           int
	markClear    bool
	nodes        map[int]*kvnode
	stores       map[int]*store
	slotOutCount int //待迁出的slot数量
	slotInCount  int //待迁入的slot数量
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
			nj := KvNodeJson{
				NodeID:      vv.id,
				Host:        vv.host,
				ServicePort: vv.servicePort,
				RaftPort:    vv.raftPort,
				Store:       vv.store,
			}

			setJson.KvNodes = append(setJson.KvNodes, nj)
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
				store:       vv.Store,
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

		if len(v.Nodes) != MinReplicaPerSet {
			return fmt.Errorf("node count of set should be %d", MinReplicaPerSet)
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
				store:       map[int]*FlyKvStoreState{},
			}
			s.nodes[int(vv.NodeID)] = n
		}

		for j := 0; j < StorePerSet; j++ {
			st := &store{
				id:    j + 1,
				slots: storeBitmaps[i+j],
				set:   s,
			}

			s.stores[st.id] = st
		}

		for _, vvv := range s.nodes {
			for j := 0; j < StorePerSet; j++ {
				vvv.store[j+1] = &FlyKvStoreState{
					Type:  VoterStore,
					Value: FlyKvCommited,
				}
			}
		}

		d.sets[int(v.SetID)] = s
	}

	return nil
}

type ProposalInstallDeployment struct {
	proposalBase
	D DeploymentJson
}

func (p *ProposalInstallDeployment) Serilize(b []byte) []byte {
	return serilizeProposal(b, proposalInstallDeployment, p)
}

func (p *ProposalInstallDeployment) apply(pd *pd) {
	err := func() error {
		if nil != pd.pState.MetaTransaction {
			return errors.New("wait for previous meta transaction finish")
		}

		if nil != pd.pState.deployment {
			return errors.New("already install")
		}
		return nil
	}()

	if nil == err {
		pd.pState.deployment = &deployment{}
		pd.pState.deployment.loadFromDeploymentJson(&p.D)
	}

	if nil != p.reply {
		p.reply(err)
	}
}

func (p *ProposalInstallDeployment) replay(pd *pd) {
	p.apply(pd)
}

func (p *pd) makeReplyFunc(from *net.UDPAddr, m *snet.Message, resp proto.Message) func(error) {
	return func(err error) {
		v := reflect.ValueOf(resp).Elem()
		if nil == err {
			v.FieldByName("Ok").SetBool(true)
		} else {
			v.FieldByName("Ok").SetBool(false)
			v.FieldByName("Reason").SetString(err.Error())
		}
		p.udp.SendTo(from, snet.MakeMessage(m.Context, resp.(proto.Message)))
	}
}

func (p *pd) onInstallDeployment(from *net.UDPAddr, m *snet.Message) {

	msg := m.Msg.(*sproto.InstallDeployment)
	resp := &sproto.InstallDeploymentResp{}
	var d *deployment

	err := func() error {
		if nil != p.pState.MetaTransaction {
			return errors.New("wait for previous meta transaction finish")
		}

		if nil != p.pState.deployment {
			return errors.New("already install")
		}

		d = &deployment{}
		if err := d.loadFromPB(msg.Sets); nil != err {
			return err
		}

		return nil
	}()

	if nil != err {
		resp.Ok = false
		resp.Reason = err.Error()
		p.udp.SendTo(from, snet.MakeMessage(m.Context, resp))
	} else {
		p.issueProposal(&ProposalInstallDeployment{
			D: d.toDeploymentJson(),
			proposalBase: proposalBase{
				reply: p.makeReplyFunc(from, m, resp),
			},
		})
	}
}
