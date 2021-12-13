package flypd

import (
	"crypto/md5"
	"encoding/base64"
	"encoding/json"
	"errors"
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
	context  int64
	timer    *time.Timer
}

type RemovingNode struct {
	NodeID   int
	SetID    int
	OkStores []int
	context  int64
	timer    *time.Timer
}

type flygate struct {
	service       string
	msgPerSecond  int
	deadlineTimer *time.Timer
	token         string
}

type flygateMgr struct {
	flygateMap map[string]*flygate
	mainque    applicationQueue
}

func (f *flygateMgr) getFlyGate() (ret []string) {
	for k, _ := range f.flygateMap {
		ret = append(ret, k)
	}
	return
}

func (f *flygateMgr) onFlyGateTimeout(gateService string, t *time.Timer) {
	if v, ok := f.flygateMap[gateService]; ok && v.deadlineTimer == t {
		delete(f.flygateMap, gateService)
	}
}

func isValidTcpService(service string, token string) bool {
	if "" == service {
		return false
	}

	if _, err := net.ResolveTCPAddr("tcp", service); nil == err {
		tmp := md5.Sum([]byte(service + "magicNum"))
		if token == base64.StdEncoding.EncodeToString(tmp[:]) {
			return true
		}
	}

	return false
}

func (f *flygateMgr) onHeartBeat(gateService string, token string, msgPerSecond int) {
	var g *flygate
	var ok bool

	if g, ok = f.flygateMap[gateService]; ok {
		if g.token != token {
			return
		}
		g.deadlineTimer.Stop()
	} else {

		if "" == gateService {
			return
		}

		if _, err := net.ResolveTCPAddr("tcp", gateService); nil != err {
			return
		}

		tmp := md5.Sum([]byte(gateService + "magicNum"))
		if token != base64.StdEncoding.EncodeToString(tmp[:]) {
			return
		}

		g = &flygate{
			service: gateService,
			token:   token,
		}
		f.flygateMap[gateService] = g
	}

	var deadlineTimer *time.Timer

	deadlineTimer = time.AfterFunc(time.Second*10, func() {
		f.mainque.AppendHighestPriotiryItem(func() {
			f.onFlyGateTimeout(gateService, deadlineTimer)
		})
	})

	g.deadlineTimer = deadlineTimer
	g.msgPerSecond = msgPerSecond
}

type persistenceState struct {
	Deployment      DeploymentJson
	AddingNode      map[int]*AddingNode
	RemovingNode    map[int]*RemovingNode
	SlotTransfer    map[int]*TransSlotTransfer
	Meta            Meta
	MetaTransaction *MetaTransaction
}

type pd struct {
	id              int
	leader          int
	rn              *raft.RaftNode
	mutilRaft       *raft.MutilRaft
	mainque         applicationQueue
	udp             *flynet.Udp
	markClearSet    map[int]*set
	msgHandler      map[reflect.Type]func(*net.UDPAddr, *snet.Message)
	stoponce        int32
	startonce       int32
	wait            sync.WaitGroup
	ready           bool
	flygateMgr      flygateMgr
	raftCluster     string
	config          *Config
	udpService      string
	onBalanceFinish func()
	deployment      *deployment
	pState          persistenceState
}

func NewPd(id int, config *Config, udpService string, cluster string) *pd {

	mainQueue := applicationQueue{
		q: queue.NewPriorityQueue(2, 10000),
	}

	p := &pd{
		id:           id,
		mainque:      mainQueue,
		msgHandler:   map[reflect.Type]func(*net.UDPAddr, *snet.Message){},
		markClearSet: map[int]*set{},
		flygateMgr: flygateMgr{
			flygateMap: map[string]*flygate{},
			mainque:    mainQueue,
		},
		raftCluster: cluster,
		config:      config,
		udpService:  udpService,
		pState: persistenceState{
			AddingNode:   map[int]*AddingNode{},
			RemovingNode: map[int]*RemovingNode{},
			SlotTransfer: map[int]*TransSlotTransfer{},
		},
	}

	p.initMsgHandler()

	return p
}

func (q applicationQueue) AppendHighestPriotiryItem(m interface{}) {
	if err := q.q.ForceAppend(1, m); nil != err {
		GetSugar().Errorf("%v", err)
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
	if nil != p.deployment {
		for _, v := range p.deployment.sets {
			if n, ok := v.nodes[int(nodeID)]; ok {
				return n
			}
		}
	}
	return nil
}

func (p *pd) slotBalance() {
	if len(p.pState.SlotTransfer) >= CurrentTransferCount {
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

	lSets, lMCSets := len(p.deployment.sets), len(p.markClearSet)

	setAverageSlotCount := slot.SlotCount / (lSets - lMCSets)

	storeAverageSlotCount := slot.SlotCount / ((lSets - lMCSets) * StorePerSet)

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
				if _, ok := p.pState.SlotTransfer[v]; !ok {
					p.beginSlotTransfer(v, outStore.set.id, outStore.id, inStore.set.id, inStore.id)
					return
				}
			}
		}
	}

	if len(p.pState.SlotTransfer) == 0 && nil != p.onBalanceFinish {
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

	GetSugar().Infof("flypd start udp at %s", p.udpService)

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
					if p.isLeader() && p.ready {
						p.onMsg(from, msg.(*snet.Message))
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
					if t, ok := p.pState.SlotTransfer[vvv]; ok {
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

		for _, v := range p.pState.AddingNode {
			p.sendNotifyAddNode(v)
			v.timer = time.AfterFunc(time.Second*3, func() {
				p.mainque.AppendHighestPriotiryItem(v)
			})
		}

		for _, v := range p.pState.RemovingNode {
			p.sendNotifyRemNode(v)
			v.timer = time.AfterFunc(time.Second*3, func() {
				p.mainque.AppendHighestPriotiryItem(v)
			})
		}

		for _, v := range p.pState.SlotTransfer {
			v.notify()
		}
	}

	if nil != p.pState.MetaTransaction {
		if p.pState.MetaTransaction.Prepareing {
			//上次做leader时尚未提交的事务，在下次成为leader时要清除
			p.pState.MetaTransaction = nil
		} else {
			p.pState.MetaTransaction.notifyStore(p)
		}
	}

}

func (p *pd) onLeaderDemote() {
	for _, v := range p.pState.AddingNode {
		v.timer.Stop()
		v.timer = nil
	}

	for _, v := range p.pState.RemovingNode {
		v.timer.Stop()
		v.timer = nil
	}

	for _, v := range p.pState.SlotTransfer {
		v.timer.Stop()
		v.timer = nil
	}

	if nil != p.pState.MetaTransaction && nil != p.pState.MetaTransaction.timer {
		p.pState.MetaTransaction.timer.Stop()
		p.pState.MetaTransaction.timer = nil
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
	ann := p.pState.AddingNode[an.NodeID]
	if ann == an {
		p.sendNotifyAddNode(an)
		an.timer = time.AfterFunc(time.Second*3, func() {
			p.mainque.AppendHighestPriotiryItem(an)
		})
	}
}

func (p *pd) onRemNodeTimeout(rn *RemovingNode) {
	rnn := p.pState.RemovingNode[rn.NodeID]
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
					if _, ok := p.pState.SlotTransfer[v.(*TransSlotTransfer).Slot]; ok {
						v.(*TransSlotTransfer).notify()
					}
				}
			case *MetaTransaction:
				if p.isLeader() {
					v.(*MetaTransaction).notifyStore(p)
				}
			default:
				GetSugar().Infof("here %v %s", v, reflect.TypeOf(v).String())
			}
		}
	}()

}

func (p *pd) getSnapshot() ([]byte, error) {

	p.pState.Deployment = p.deployment.toDeploymentJson()

	persistenceState, err := json.Marshal(&p.pState)

	if nil != err {
		return nil, err
	}

	b := make([]byte, 0, 4+len(persistenceState))

	b = buffer.AppendInt32(b, int32(len(persistenceState)))
	b = buffer.AppendBytes(b, persistenceState)
	return b, nil
}

func (p *pd) recoverFromSnapshot(b []byte) error {
	reader := buffer.NewReader(b)
	l := reader.GetInt32()

	err := json.Unmarshal(reader.GetBytes(int(l)), &p.pState)

	if nil != err {
		return err
	}

	err = p.deployment.loadFromDeploymentJson(&p.pState.Deployment)

	if nil != err {
		return err
	}

	for _, v := range p.pState.SlotTransfer {
		v.pd = p
	}

	for _, v := range p.deployment.sets {
		if v.markClear {
			p.markClearSet[v.id] = v
		}
	}

	return nil
}
