package kvpd

import (
	"fmt"
	"strings"
)

//id范围1-9999

const (
	BaseServicePort = 10000 //对客户端服务基端口
	BaseRaftPort    = 20000 //内部raft基端口
	MaxSlotSize     = 65536
)

func IsVaildNodeID(id int) bool {
	return id >= 1 && id <= 9999
}

type Bitmap []byte

func NewBitmap(bitcount int) Bitmap {
	var count int
	if bitcount%8 == 0 {
		count = bitcount / 8
	} else {
		count = bitcount/8 + 1
	}
	return (Bitmap)(make([]byte, count))
}

func NewBitmapFromBytes(b []byte) Bitmap {
	bb := make([]byte, len(b))
	copy(bb, b)
	return (Bitmap)(bb)
}

func (this *Bitmap) Set(idx int) error {
	if idx > 0 && idx <= len(*this)*8 {
		offset := (idx - 1) / 8
		idx = (idx - 1) % 8
		(*this)[offset] = byte(1<<idx) | (*this)[offset]
		return nil
	} else {
		return fmt.Errorf("over range")
	}

}

func (this *Bitmap) Get(idx int) (int, error) {
	if idx > 0 && idx <= len(*this)*8 {
		offset := (idx - 1) / 8
		idx = (idx - 1) % 8
		b := byte(1<<idx) & (*this)[offset]
		if b > 0 {
			return 1, nil
		} else {
			return 0, nil
		}
	} else {
		return 0, fmt.Errorf("over range")
	}
}

func (this *Bitmap) Clear(idx int) error {
	if idx > 0 && idx <= len(*this)*8 {
		offset := (idx - 1) / 8
		idx = (idx - 1) % 8
		(*this)[offset] &= byte(1<<idx) ^ byte(0xFF)
		return nil
	} else {
		return fmt.Errorf("over range")
	}
}

func (this *Bitmap) ShowBit() string {
	byteStrs := []string{}
	for i, _ := range *this {
		var s string
		for j := i*8 + 1; j <= i*8+8; j++ {
			b, _ := this.Get(j)
			if b == 1 {
				s += "1"
			} else {
				s += "0"
			}
		}
		byteStrs = append(byteStrs, s)
	}

	return strings.Join(byteStrs, " ")
}

func (this *Bitmap) Show() string {
	s := []string{}
	for i := 1; i <= len(*this)*8; i++ {
		r, _ := this.Get(i)
		if r == 1 {
			s = append(s, fmt.Sprintf("%d", i))
		}
	}
	return strings.Join(s, ",")
}

func (this *Bitmap) Bytes() []byte {
	return *this
}

//实际分配端口为Base + id
type nodeConf struct {
	id          int
	servicePort int
	raftPort    int
	ip          string //配置的ip地址
	regions     []int  //node上运行的shard
}

type nodeInfo struct {
	servicePort int
	raftAddress string
	regions     []int
	slots       []Bitmap
}

func (this nodeInfo) Show() {
	fmt.Println("servicePort", this.servicePort)
	fmt.Println("raftAddress", this.raftAddress)
	for i, v := range this.regions {
		s := []string{}
		for j := 1; j <= MaxSlotSize; j++ {
			r, _ := this.slots[i].Get(j)
			if r == 1 {
				s = append(s, fmt.Sprintf("%d", j))
			}
		}
		fmt.Println("region", v, ":", strings.Join(s, ","))
	}
}

type nodeConfMgr map[int]*nodeConf

type regionInfo struct {
	regionNo int
	leader   int               //leader节点id
	nodes    map[int]*nodeConf //region关联的node
	slots    Bitmap            //region分配的slot
}

func (this regionInfo) Show() {
	n := []int{}
	for _, v := range this.nodes {
		n = append(n, v.id)
	}
	fmt.Println("region", this.regionNo)
	fmt.Println("nodes", n)
	fmt.Println("leader", this.leader)
	fmt.Println("slots:", this.slots.Show())
}

type regionMgr struct {
	regions      map[int]*regionInfo
	slotToRegion map[int]*regionInfo
}

func (this *regionMgr) getRegionSlots(region int) (Bitmap, error) {
	if r, ok := this.regions[region]; !ok {
		return nil, fmt.Errorf("region不存在")
	} else {
		return NewBitmapFromBytes(r.slots.Bytes()), nil
	}
}

func (this *regionMgr) addRegion(region int, slots []int) error {
	if _, ok := this.regions[region]; !ok {
		//不允许不同的region有重叠的slot
		for _, v := range slots {
			r, _ := this.getRegionSlots(v)
			if nil != r {
				return fmt.Errorf("slot重叠")
			}
		}

		r := &regionInfo{
			regionNo: region,
			nodes:    map[int]*nodeConf{},
			slots:    NewBitmap(MaxSlotSize),
		}

		this.regions[region] = r

		for _, v := range slots {
			if v > 0 && v <= MaxSlotSize {
				this.slotToRegion[v] = r
				r.slots.Set(v)
			}
		}

		return nil

	} else {
		return fmt.Errorf("重复的region:%d", region)
	}
}

//获得当前管理slot的region信息
func (this *regionMgr) GetSlotRegion(slot int) *regionInfo {
	if r, ok := this.slotToRegion[slot]; ok {
		return r
	} else {
		return nil
	}
}

//向region添加一个node
func (this *regionMgr) addNode(region int, node *nodeConf) error {
	r, ok := this.regions[region]
	if !ok {
		return fmt.Errorf("region不存在")
	}

	r.nodes[node.id] = node
	return nil
}

//获取region关联的所有node
func (this *regionMgr) getRegionNodes(region int) ([]*nodeConf, error) {
	r, ok := this.regions[region]
	if !ok {
		return nil, fmt.Errorf("region不存在")
	}

	nodes := []*nodeConf{}
	for _, v := range r.nodes {
		nodes = append(nodes, v)
	}
	return nodes, nil
}

//获得node启动相关参数
func (this *nodeConfMgr) getInfoByID(rMgr *regionMgr, id int) (*nodeInfo, error) {
	if c, ok := (*this)[id]; ok {
		info := &nodeInfo{
			regions:     c.regions,
			slots:       []Bitmap{},
			servicePort: c.servicePort,
		}

		nodes := map[int]*nodeConf{}
		for _, v := range c.regions {
			n, _ := rMgr.getRegionNodes(v)
			for _, vv := range n {
				nodes[vv.id] = vv
			}
		}

		raftAddress := []string{}

		for _, v := range nodes {
			raftAddress = append(raftAddress, fmt.Sprintf("%d@http://%s:%d", v.id, v.ip, v.raftPort))
		}

		info.raftAddress = strings.Join(raftAddress, ",")

		for _, v := range info.regions {
			b, err := rMgr.getRegionSlots(v)
			if nil != err {
				return nil, err
			} else {
				info.slots = append(info.slots, b)
			}
		}

		return info, nil

	} else {
		return nil, fmt.Errorf("节点不存在")
	}
}

func (this *nodeConfMgr) getConfByID(id int) *nodeConf {
	if c, ok := (*this)[id]; ok {
		return c
	} else {
		return nil
	}
}

func (this *nodeConfMgr) addNodeConf(id int, ip string, regions []int, rMgr *regionMgr) error {
	if !IsVaildNodeID(id) {
		return fmt.Errorf("非法nodeid")
	}

	if _, ok := (*this)[id]; ok {
		return fmt.Errorf("重复nodeid")
	} else {
		n := &nodeConf{
			id:          id,
			ip:          ip,
			regions:     make([]int, len(regions)),
			servicePort: BaseServicePort + id,
			raftPort:    BaseRaftPort + id,
		}
		copy(n.regions, regions)
		for _, v := range regions {
			if err := rMgr.addNode(v, n); nil != err {
				return err
			}
		}
		(*this)[id] = n
		return nil
	}
}
