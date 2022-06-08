package flypd

import (
	"encoding/json"
	"fmt"
	"github.com/sniperHW/flyfish/pkg/bitmap"
	snet "github.com/sniperHW/flyfish/server/net"
	sproto "github.com/sniperHW/flyfish/server/proto"
	"github.com/sniperHW/flyfish/server/slot"
	"net"
	"os"
	"sort"
	"time"
)

var StorePerSet int = 3      //每个set含有多少个store
var MinReplicaPerSet int = 1 //最少副本数

type StoreType uint16

const (
	AddLearnerStore = StoreType(1)
	LearnerStore    = StoreType(2)
	VoterStore      = StoreType(3)
	RemovingStore   = StoreType(4)
)

type NodeStoreState struct {
	StoreType             StoreType
	RaftID                uint64
	isLead                bool
	StoreTypeBeforeRemove StoreType
	kvcount               int
	lastReport            time.Time
	progress              uint64
	metaVersion           int64
	notifying             int32
	halt                  bool
}

func (nss *NodeStoreState) isActive() bool {
	if nss.lastReport.IsZero() {
		return false
	} else if time.Now().Sub(nss.lastReport) > time.Second*3 {
		return false
	} else {
		return true
	}
}

func (nss *NodeStoreState) isLeader() bool {
	if !nss.isActive() {
		nss.isLead = false
	}
	return nss.isLead
}

func (nss *NodeStoreState) isLearner() bool {
	return nss.StoreType == LearnerStore || nss.StoreType == AddLearnerStore
}

type KvNode struct {
	NodeID      int
	Host        string
	ServicePort int
	RaftPort    int
	Store       map[int]*NodeStoreState
}

func (n *KvNode) isLearner(store int) bool {
	s, ok := n.Store[store]
	return ok && s.isLearner()
}

func (n *KvNode) isLeader(store int) bool {
	s, ok := n.Store[store]
	return ok && s.isLeader()
}

func (n *KvNode) leaderCount() (leaderCount int) {
	for _, v := range n.Store {
		if v.isLeader() {
			leaderCount++
		}
	}
	return
}

func (n *KvNode) canTransferLeader(store int) bool {
	s, ok := n.Store[store]
	return ok && s.StoreType == VoterStore
}

func (n *KvNode) getRaftID(store int) uint64 {
	s, ok := n.Store[store]
	if ok {
		return s.RaftID
	} else {
		return 0
	}
}

type Store struct {
	StoreID int
	Slots   []byte
	slots   *bitmap.Bitmap
}

type Set struct {
	Version   int64
	SetID     int
	Nodes     map[int]*KvNode
	Stores    map[int]*Store
	MarkClear bool //需要将其上slot全部移走
}

func (s *Set) getTotalSlotCount() int {
	totalSlotCount := 0
	for _, v := range s.Stores {
		totalSlotCount += len(v.slots.GetOpenBits())
	}
	return totalSlotCount
}

//将leader均分到kvnode
func (s *Set) storeBalance(pd *pd) {
	leaderPerSet := StorePerSet / len(s.Nodes)
	if StorePerSet%len(s.Nodes) != 0 {
		leaderPerSet++
	}

	var maxNode *KvNode
	max := 0

	for _, v := range s.Nodes {
		lc := v.leaderCount()
		if lc > max {
			max = lc
			maxNode = v
		}
	}

	//GetSugar().Infof("storeBalance set:%d leaderPerSet:%d max:%d StorePerSet:%d len(s.nodes):%d", s.id, leaderPerSet, max, StorePerSet, len(s.nodes))

	if max > leaderPerSet {
		for store, state := range maxNode.Store {
			if !state.isLeader() {
				continue
			}
			var candidates []*KvNode
			for _, v := range s.Nodes {
				if v != maxNode && v.canTransferLeader(store) && v.leaderCount()+1 <= leaderPerSet {
					candidates = append(candidates, v)
				}
			}

			if len(candidates) > 0 {
				sort.Slice(candidates, func(i, j int) bool {
					return candidates[i].NodeID < candidates[j].NodeID
				})
				transferee := candidates[0]
				if addr, err := net.ResolveUDPAddr("udp", fmt.Sprintf("%s:%d", maxNode.Host, maxNode.ServicePort)); nil == err {
					pd.udp.SendTo(addr, snet.MakeMessage(0, &sproto.TrasnferLeader{
						StoreID:    int32(store),
						Transferee: transferee.getRaftID(store),
					}))
					return
				}
			}
		}
	}
}

type Deployment struct {
	Version int64
	Sets    map[int]*Set
}

func (d Deployment) queryRouteInfo(req *sproto.QueryRouteInfo) *sproto.QueryRouteInfoResp {
	resp := &sproto.QueryRouteInfoResp{}
	resp.Version = d.Version
	if req.Version >= d.Version {
		//路由信息没有发生过变更
	} else {
		var localSets []int32
		for _, v := range d.Sets {
			localSets = append(localSets, int32(v.SetID))
			if v.Version > req.Version {
				s := &sproto.RouteInfoSet{
					SetID: int32(v.SetID),
				}

				for _, vv := range v.Nodes {
					s.Kvnodes = append(s.Kvnodes, &sproto.RouteInfoKvNode{
						NodeID:      int32(vv.NodeID),
						Host:        vv.Host,
						ServicePort: int32(vv.ServicePort),
					})
				}

				for _, vv := range v.Stores {
					s.Stores = append(s.Stores, int32(vv.StoreID))
					s.Slots = append(s.Slots, vv.Slots)
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

func (d Deployment) toJson() ([]byte, error) {
	if data, err := json.Marshal(d); err != nil {
		return nil, err
	} else {
		return data, nil
	}
}

func (d Deployment) ToPrettyJson() ([]byte, error) {
	if data, err := json.MarshalIndent(d, "", "    "); err != nil {
		return nil, err
	} else {
		return data, nil
	}
}

func (d Deployment) check() error {
	nodes := map[int]bool{}
	services := map[string]bool{}
	raftServices := map[string]bool{}
	for _, set := range d.Sets {
		if len(set.Nodes) < MinReplicaPerSet {
			return fmt.Errorf("node size perset must >= %d", MinReplicaPerSet)
		}

		for _, node := range set.Nodes {
			if nodes[node.NodeID] {
				return fmt.Errorf("duplicate node %d", node.NodeID)
			}
			nodes[node.NodeID] = true

			service := fmt.Sprintf("%s:%d", node.Host, node.ServicePort)
			if services[service] {
				return fmt.Errorf("duplicate service %s", service)
			}
			services[service] = true

			raftService := fmt.Sprintf("%s:%d", node.Host, node.RaftPort)
			if raftServices[raftService] {
				return fmt.Errorf("duplicate raftService %s", raftService)
			}
			raftServices[raftService] = true
		}
	}
	return nil
}

type ProposalInstallDeployment struct {
	proposalBase
	Deployment *Deployment
}

func (p *ProposalInstallDeployment) Serilize(b []byte) []byte {
	return serilizeProposal(b, proposalInstallDeployment, p)
}

func (p *ProposalInstallDeployment) apply(pd *pd) {
	pd.Deployment = *p.Deployment

	for _, set := range p.Deployment.Sets {
		for _, store := range set.Stores {
			store.slots, _ = bitmap.CreateFromJson(store.Slots)
		}
	}
}

func (p *pd) loadInitDeployment() {
	GetSugar().Infof("loadInitDeployment")
	if "" != p.config.InitDepoymentPath {
		f, err := os.Open(p.config.InitDepoymentPath)
		if nil == err {
			var b []byte
			for {
				data := make([]byte, 4096)
				count, err := f.Read(data)
				if count > 0 {
					b = append(b, data[:count]...)
				}

				if nil != err {
					break
				}
			}

			var deployment Deployment
			var err error
			if err = json.Unmarshal(b, &deployment); err != nil {
				GetSugar().Panicf("loadInitDeployment err:%v", err)
				return
			}

			deployment.Version = 1

			if len(deployment.Sets) > 0 {
				storeCount := len(deployment.Sets) * StorePerSet
				var storeBitmaps []*bitmap.Bitmap

				for i := 0; i < storeCount; i++ {
					storeBitmaps = append(storeBitmaps, bitmap.New(slot.SlotCount))
				}

				jj := 0
				for i := 0; i < slot.SlotCount; i++ {
					storeBitmaps[jj].Set(i)
					jj = (jj + 1) % storeCount
				}

				i := -1
				for _, set := range deployment.Sets {
					i++
					set.Version = deployment.Version
					set.Stores = map[int]*Store{}

					for j := 0; j < StorePerSet; j++ {
						set.Stores[j+1] = &Store{
							StoreID: j + 1,
							Slots:   storeBitmaps[i*StorePerSet+j].ToJson(),
						}
					}

					for _, node := range set.Nodes {
						node.Store = map[int]*NodeStoreState{}
						for j := 0; j < StorePerSet; j++ {
							node.Store[j+1] = &NodeStoreState{
								StoreType: VoterStore,
								RaftID:    p.raftIDGen.Next(),
							}
						}
					}
				}

				if err = deployment.check(); nil != err {
					GetSugar().Panicf("loadInitDeployment err:%v", err)
					return
				}
			}

			//for _, v := range deployment.Sets[1].Stores {
			//	GetSugar().Infof("%v", v)
			//}

			p.issueProposal(&ProposalInstallDeployment{
				Deployment: &deployment,
			})

		} else {
			GetSugar().Panicf("loadInitDeployment err:%v", err)
		}
	} else {
		p.issueProposal(&ProposalInstallDeployment{
			Deployment: &Deployment{
				Version: 1,
				Sets:    map[int]*Set{},
			},
		})
	}
}
