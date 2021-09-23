package flypd

import (
	//"github.com/sniperHW/flyfish/server/slot"
	"encoding/json"
	"github.com/sniperHW/flyfish/pkg/bitmap"
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
	}

	return nil

}
