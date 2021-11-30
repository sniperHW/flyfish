package flypd

import (
	"encoding/json"
	"errors"
	"github.com/gogo/protobuf/proto"
	"github.com/sniperHW/flyfish/pkg/buffer"
	flynet "github.com/sniperHW/flyfish/pkg/net"
	"github.com/sniperHW/flyfish/pkg/queue"
	"github.com/sniperHW/flyfish/pkg/raft"
	snet "github.com/sniperHW/flyfish/server/net"
	"github.com/sniperHW/flyfish/server/slot"
	"go.etcd.io/etcd/raft/raftpb"
	"net"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type applicationQueue struct {
	q *queue.PriorityQueue
}

type AddingNode struct {
	KvNodeJson
	SetID    int
	OkStores []int
	timer    *time.Timer
}

type RemovingNode struct {
	NodeID   int
	SetID    int
	OkStores []int
	timer    *time.Timer
}

type flygate struct {
	service       string
	deadlineTimer *time.Timer
}

type flygateMgr struct {
	flygateMap   map[string]*flygate
	flygateArray []*flygate
	next         int
	mainque      applicationQueue
}

func (f *flygateMgr) getFlyGate() *flygate {
	if len(f.flygateArray) == 0 {
		return nil
	} else {
		f.next++
		return f.flygateArray[f.next%len(f.flygateArray)]
	}
}

func (f *flygateMgr) onFlyGateTimeout(gateService string, t *time.Timer) {
	if v, ok := f.flygateMap[gateService]; ok && v.deadlineTimer == t {
		delete(f.flygateMap, gateService)
		for i, vv := range f.flygateArray {
			if vv == v {
				f.flygateArray[i], f.flygateArray[len(f.flygateArray)-1] = f.flygateArray[len(f.flygateArray)-1], f.flygateArray[i]
				f.flygateArray = f.flygateArray[:len(f.flygateArray)-1]
				break
			}
		}
	}
}

func (f *flygateMgr) onQueryRouteInfo(gateService string) {
	var g *flygate
	var ok bool

	if g, ok = f.flygateMap[gateService]; ok {
		g.deadlineTimer.Stop()
	} else {
		g = &flygate{
			service: gateService,
		}
		f.flygateMap[gateService] = g
		f.flygateArray = append(f.flygateArray, g)
	}

	var deadlineTimer *time.Timer

	deadlineTimer = time.AfterFunc(time.Second*10, func() {
		f.mainque.AppendHighestPriotiryItem(func() {
			f.onFlyGateTimeout(gateService, deadlineTimer)
		})
	})

	g.deadlineTimer = deadlineTimer
}

type pd struct {
	id              int
	leader          int
	rn              *raft.RaftNode
	mutilRaft       *raft.MutilRaft
	mainque         applicationQueue
	udp             *flynet.Udp
	deployment      *deployment
	addingNode      map[int]*AddingNode
	removingNode    map[int]*RemovingNode
	slotTransfer    map[int]*TransSlotTransfer
	markClearSet    map[int]*set
	msgHandler      map[reflect.Type]func(*net.UDPAddr, proto.Message)
	stoponce        int32
	startonce       int32
	wait            sync.WaitGroup
	ready           bool
	flygateMgr      flygateMgr
	raftCluster     string
	config          *Config
	udpService      string
	onBalanceFinish func()
}

func NewPd(config *Config, udpService string, id int, cluster string) *pd {

	mainQueue := applicationQueue{
		q: queue.NewPriorityQueue(2, config.MainQueueMaxSize),
	}

	p := &pd{
		id:           id,
		mainque:      mainQueue,
		msgHandler:   map[reflect.Type]func(*net.UDPAddr, proto.Message){},
		addingNode:   map[int]*AddingNode{},
		removingNode: map[int]*RemovingNode{},
		slotTransfer: map[int]*TransSlotTransfer{},
		markClearSet: map[int]*set{},
		flygateMgr: flygateMgr{
			flygateMap: map[string]*flygate{},
			mainque:    mainQueue,
		},
		raftCluster: cluster,
		config:      config,
		udpService:  udpService,
	}

	p.initMsgHandler()

	return p
}

func (q applicationQueue) AppendHighestPriotiryItem(m interface{}) {
	if err := q.q.ForceAppend(1, m); nil != err {
		panic(err)
	}
}

func (q applicationQueue) append(m interface{}) error {
	return q.q.ForceAppend(0, m)
}

func (q applicationQueue) pop() (closed bool, v interface{}) {
	return q.q.Pop()
}

func (q applicationQueue) close() {
	q.q.Close()
}

func (p *pd) getNode(nodeID int32) *kvnode {
	for _, v := range p.deployment.sets {
		if n, ok := v.nodes[int(nodeID)]; ok {
			return n
		}
	}
	return nil
}

func (p *pd) slotBalance() {
	if len(p.slotTransfer) >= CurrentTransferCount {
		return
	}

	var outStore *store

	if len(p.markClearSet) > 0 {
		for _, v := range p.markClearSet {
			if v.getTotalSlotCount()-v.SlotOutCount > 0 {
				for _, vv := range v.stores {
					if len(vv.slots.GetOpenBits())-vv.SlotOutCount > 0 {
						outStore = vv
						break
					}
				}
			}
			if nil != outStore {
				break
			}
		}
	}

	setAverageSlotCount := slot.SlotCount / (len(p.deployment.sets) - len(p.markClearSet))

	storeAverageSlotCount := slot.SlotCount / ((len(p.deployment.sets) - len(p.markClearSet)) * StorePerSet)

	if nil == outStore {
		for _, v := range p.deployment.sets {
			if !v.markClear && v.getTotalSlotCount()-v.SlotOutCount > setAverageSlotCount+1 {
				for _, vv := range v.stores {
					if len(vv.slots.GetOpenBits())-vv.SlotOutCount > storeAverageSlotCount+1 {
						outStore = vv
						break
					}
				}
			}
			if nil != outStore {
				break
			}
		}
	}

	if nil != outStore {
		var inStore *store
		for _, v := range p.deployment.sets {
			if !v.markClear && v.getTotalSlotCount()-v.SlotInCount < setAverageSlotCount+1 {
				for _, vv := range v.stores {
					if len(vv.slots.GetOpenBits())-vv.SlotInCount < storeAverageSlotCount+1 {
						inStore = vv
						break
					}
				}
			}
			if nil != inStore {
				break
			}
		}

		if nil != inStore && nil != outStore {
			//从outStore选出一个slot
			for _, v := range outStore.slots.GetOpenBits() {
				if _, ok := p.slotTransfer[v]; !ok {
					p.beginSlotTransfer(v, outStore.set.id, outStore.id, inStore.set.id, inStore.id)
					return
				}
			}
		}
	}

	if len(p.slotTransfer) == 0 && nil != p.onBalanceFinish {
		p.onBalanceFinish()
	}

}

func (p *pd) Start() error {
	clusterArray := strings.Split(p.raftCluster, ",")

	peers := map[int]string{}

	var selfUrl string

	for _, v := range clusterArray {
		t := strings.Split(v, "@")
		if len(t) != 2 {
			panic("invaild peer")
		}
		i, err := strconv.Atoi(t[0])
		if nil != err {
			panic(err)
		}
		peers[i] = t[1]
		if i == p.id {
			selfUrl = t[1]
		}
	}

	if selfUrl == "" {
		return errors.New("cluster not contain self")
	}

	p.mutilRaft = raft.NewMutilRaft()

	p.rn = raft.NewRaftNode(p.mutilRaft, p.mainque, (p.id<<16)+1, peers, false, p.config.RaftLogDir, p.config.RaftLogPrefix)

	if err := p.startUdpService(); nil != err {
		p.rn.Stop()
		return err
	}

	GetSugar().Infof("mutilRaft serve on:%s", selfUrl)

	go p.mutilRaft.Serve(selfUrl)

	p.wait.Add(1)
	go p.serve()

	return nil
}

func (p *pd) isLeader() bool {
	return p.leader == p.rn.ID()
}

func (p *pd) issueProposal(proposal raft.Proposal) error {
	return p.rn.IssueProposal(proposal)
}

func (p *pd) startUdpService() error {
	udp, err := flynet.NewUdp(p.udpService, snet.Pack, snet.Unpack)
	if nil != err {
		return err
	}

	p.udp = udp

	go func() {
		recvbuff := make([]byte, 64*1024)
		for {
			from, msg, err := udp.ReadFrom(recvbuff)
			if nil != err {
				GetSugar().Errorf("read err:%v", err)
				return
			} else {
				//GetSugar().Infof("got msg")
				p.mainque.append(func() {
					if p.isLeader() && p.ready {
						p.onMsg(from, msg)
					} else {
						GetSugar().Infof("drop msg")
					}
				})
			}
		}
	}()

	return nil
}

func (p *pd) onBecomeLeader() {

	if nil != p.deployment {

		//重置slotBalance相关的临时数据
		for _, v := range p.deployment.sets {
			v.SlotOutCount = 0
			v.SlotInCount = 0
			for _, vv := range v.stores {
				vv.SlotOutCount = 0
				vv.SlotInCount = 0
				for _, vvv := range vv.slots.GetOpenBits() {
					if t, ok := p.slotTransfer[vvv]; ok {
						if vvv == t.StoreTransferIn {
							vv.SlotInCount++
							v.SlotInCount++
						} else if vvv == t.StoreTransferOut {
							vv.SlotOutCount++
							v.SlotOutCount++
						}
					}
				}
			}
		}

		p.slotBalance()

		for _, v := range p.addingNode {
			p.sendNotifyAddNode(v)
			v.timer = time.AfterFunc(time.Second*3, func() {
				p.mainque.AppendHighestPriotiryItem(v)
			})
		}

		for _, v := range p.removingNode {
			p.sendNotifyRemNode(v)
			v.timer = time.AfterFunc(time.Second*3, func() {
				p.mainque.AppendHighestPriotiryItem(v)
			})
		}

		for _, v := range p.slotTransfer {
			v.notify()
		}
	}
}

func (p *pd) onLeaderDemote() {
	for _, v := range p.addingNode {
		v.timer.Stop()
		v.timer = nil
	}

	for _, v := range p.removingNode {
		v.timer.Stop()
		v.timer = nil
	}

	for _, v := range p.slotTransfer {
		v.timer.Stop()
		v.timer = nil
	}
}

func (p *pd) Stop() {
	if atomic.CompareAndSwapInt32(&p.stoponce, 0, 1) {
		GetSugar().Info("Stop")
		p.udp.Close()
		p.rn.Stop()
		p.mutilRaft.Stop()
		p.wait.Wait()
	}
}

func (p *pd) processCommited(commited raft.Committed) {
	if len(commited.Proposals) > 0 {
		for _, v := range commited.Proposals {
			v.(applyable).apply()
		}
	} else {
		err := p.replayProposal(commited.Data)
		if nil != err {
			GetSugar().Panic(err)
		}
	}

	//raft请求snapshot,建立snapshot并返回
	snapshotNotify := commited.GetSnapshotNotify()
	if nil != snapshotNotify {
		snapshot, err := p.getSnapshot()
		if nil != err {
			GetSugar().Panic(err)
		}
		snapshotNotify.Notify(snapshot)
	}
}

func (p *pd) onAddNodeTimeout(an *AddingNode) {
	ann := p.addingNode[an.NodeID]
	if ann == an {
		p.sendNotifyAddNode(an)
		an.timer = time.AfterFunc(time.Second*3, func() {
			p.mainque.AppendHighestPriotiryItem(an)
		})
	}
}

func (p *pd) onRemNodeTimeout(rn *RemovingNode) {
	rnn := p.removingNode[rn.NodeID]
	if rnn == rn {
		p.sendNotifyRemNode(rn)
		rn.timer = time.AfterFunc(time.Second*3, func() {
			p.mainque.AppendHighestPriotiryItem(rn)
		})
	}
}

func (p *pd) serve() {

	go func() {
		defer func() {
			p.wait.Done()
			p.mainque.close()
		}()
		for {
			_, v := p.mainque.pop()
			switch v.(type) {
			case error:
				GetSugar().Errorf("error for raft:%v", v.(error))
				return
			case func():
				v.(func())()
			case raft.Committed:
				p.processCommited(v.(raft.Committed))
			case []raft.LinearizableRead:
			case raft.ProposalConfChange:
			case raft.ConfChange:
				c := v.(raft.ConfChange)
				if c.CCType == raftpb.ConfChangeRemoveNode && c.NodeID == p.id {
					GetSugar().Info("RemoveFromCluster")
					return
				}
			case raft.ReplayOK:
				p.ready = true
			case raft.RaftStopOK:
				GetSugar().Info("RaftStopOK")
				return
			case raftpb.Snapshot:
				snapshot := v.(raftpb.Snapshot)
				GetSugar().Infof("%x loading snapshot at term %d and index %d", p.rn.ID(), snapshot.Metadata.Term, snapshot.Metadata.Index)
				if err := p.recoverFromSnapshot(snapshot.Data); err != nil {
					GetSugar().Panic(err)
				}
			case raft.LeaderChange:
				oldLeader := p.leader
				p.leader = v.(raft.LeaderChange).Leader
				if p.leader == p.rn.ID() {
					p.onBecomeLeader()
				}

				if oldLeader == p.rn.ID() && !p.isLeader() {
					p.onLeaderDemote()
				}

			case *AddingNode:
				if p.isLeader() {
					p.onAddNodeTimeout(v.(*AddingNode))
				}
			case *RemovingNode:
				if p.isLeader() {
					p.onRemNodeTimeout(v.(*RemovingNode))
				}
			case *TransSlotTransfer:
				if p.isLeader() {
					if _, ok := p.slotTransfer[v.(*TransSlotTransfer).Slot]; ok {
						v.(*TransSlotTransfer).notify()
					}
				}
			default:
				GetSugar().Infof("here %v %s", v, reflect.TypeOf(v).String())
			}
		}
	}()

}

func (p *pd) getSnapshot() ([]byte, error) {

	jsonDeployment, err := p.deployment.toJson()
	if nil != err {
		return nil, err
	}

	var addingNode []*AddingNode
	for _, v := range p.addingNode {
		addingNode = append(addingNode, v)
	}

	jsonAddingNode, err := json.Marshal(&addingNode)

	if nil != err {
		return nil, err
	}

	var removingNode []*RemovingNode
	for _, v := range p.removingNode {
		removingNode = append(removingNode, v)
	}

	jsonRemovingNode, err := json.Marshal(&removingNode)

	if nil != err {
		return nil, err
	}

	var slotTransfer []*TransSlotTransfer
	for _, v := range p.slotTransfer {
		slotTransfer = append(slotTransfer, v)
	}

	jsonSlotTransfer, err := json.Marshal(&slotTransfer)

	if nil != err {
		return nil, err
	}

	b := make([]byte, 0, 4*4+len(jsonDeployment)+len(jsonAddingNode)+len(jsonRemovingNode)+len(jsonSlotTransfer))

	b = buffer.AppendInt32(b, int32(len(jsonDeployment)))
	b = buffer.AppendBytes(b, jsonDeployment)

	b = buffer.AppendInt32(b, int32(len(jsonAddingNode)))
	b = buffer.AppendBytes(b, jsonAddingNode)

	b = buffer.AppendInt32(b, int32(len(jsonRemovingNode)))
	b = buffer.AppendBytes(b, jsonRemovingNode)

	b = buffer.AppendInt32(b, int32(len(jsonSlotTransfer)))
	b = buffer.AppendBytes(b, jsonSlotTransfer)

	return b, nil
}

func (p *pd) recoverFromSnapshot(b []byte) error {
	reader := buffer.NewReader(b)
	l1 := reader.GetInt32()

	var d deployment
	err := d.loadFromJson(reader.GetBytes(int(l1)))
	p.deployment = &d

	if nil != err {
		return err
	}

	l2 := reader.GetInt32()
	var addingNode []*AddingNode

	err = json.Unmarshal(reader.GetBytes(int(l2)), &addingNode)
	if nil != err {
		return err
	}

	for _, v := range addingNode {
		p.addingNode[int(v.NodeID)] = v
	}

	l3 := reader.GetInt32()
	var removingNode []*RemovingNode

	err = json.Unmarshal(reader.GetBytes(int(l3)), &removingNode)
	if nil != err {
		return err
	}

	for _, v := range removingNode {
		p.removingNode[int(v.NodeID)] = v
	}

	l4 := reader.GetInt32()
	var slotTransfer []*TransSlotTransfer

	err = json.Unmarshal(reader.GetBytes(int(l4)), &slotTransfer)
	if nil != err {
		return err
	}

	for _, v := range slotTransfer {
		v.pd = p
		p.slotTransfer[int(v.Slot)] = v
	}

	return nil
}
