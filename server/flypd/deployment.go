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

var StorePerSet int = 5  //每个set含有多少个store
var KvNodePerSet int = 1 //每个set含有多少kvnode

type KvNodeJson struct {
	NodeID      int
	Host        string
	ServicePort int
	InterPort   int
}

type StoreJson struct {
	StoreID int
	Slots   []byte
}

type SetJson struct {
	SetID    int
	KvNodes  []KvNodeJson
	Stores   []StoreJson
	Removing bool //set是否处于移除过程中
}

type DeploymentJson struct {
	Sets []SetJson
}

type kvnode struct {
	id          int
	host        string
	servicePort int
	interPort   int
	set         *set
}

type store struct {
	id    int
	slots *bitmap.Bitmap
	set   *set
}

type set struct {
	id       int
	removing bool
	nodes    map[int]*kvnode
	stores   map[int]*store
}

type deployment struct {
	sets map[int]*set
}

func (d deployment) toJson() ([]byte, error) {
	var deploymentJson DeploymentJson
	for _, v := range d.sets {
		setJson := SetJson{
			SetID:    v.id,
			Removing: v.removing,
		}

		for _, vv := range v.nodes {
			setJson.KvNodes = append(setJson.KvNodes, KvNodeJson{
				NodeID:      vv.id,
				Host:        vv.host,
				ServicePort: vv.servicePort,
				InterPort:   vv.interPort,
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

	for _, v := range deploymentJson.Sets {
		s := &set{
			id:       v.SetID,
			removing: v.Removing,
			nodes:    map[int]*kvnode{},
			stores:   map[int]*store{},
		}

		for _, vv := range v.KvNodes {
			n := &kvnode{
				id:          vv.NodeID,
				host:        vv.Host,
				servicePort: vv.ServicePort,
				interPort:   vv.InterPort,
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

	nodes := map[int32]bool{}
	services := map[string]bool{}
	inters := map[string]bool{}

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
			id:     int(v.SetID),
			nodes:  map[int]*kvnode{},
			stores: map[int]*store{},
		}

		for _, vv := range v.Nodes {
			if _, ok := nodes[vv.NodeID]; ok {
				return fmt.Errorf("duplicate node:%d", vv.NodeID)
			}

			service := fmt.Sprintf("%s:%d", vv.Host, vv.ServicePort)

			if _, ok := services[service]; ok {
				return fmt.Errorf("duplicate service:%s", service)
			}

			inter := fmt.Sprintf("%s:%d", vv.Host, vv.InterPort)

			if _, ok := inters[inter]; ok {
				return fmt.Errorf("duplicate inter:%s", inter)
			}

			nodes[vv.NodeID] = true
			services[service] = true
			inters[inter] = true

			n := &kvnode{
				id:          int(vv.NodeID),
				host:        vv.Host,
				servicePort: int(vv.ServicePort),
				interPort:   int(vv.InterPort),
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
			interPort:   int(v.InterPort),
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

	p.pd.deployment.sets[s.id] = s

}

func (p *ProposalAddSet) apply() {
	p.doApply()
	p.reply()
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
	p.reply()
}

func (p *pd) replayRemSet(reader *buffer.BufferReader) error {
	setID := int(reader.GetInt32())
	delete(p.deployment.sets, setID)
	return nil
}
