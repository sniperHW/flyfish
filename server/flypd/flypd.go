package flypd

import (
	"encoding/json"
	"errors"
	"github.com/sniperHW/flyfish/pkg/buffer"
	"github.com/sniperHW/flyfish/pkg/etcd/raft/raftpb"
	flynet "github.com/sniperHW/flyfish/pkg/net"
	"github.com/sniperHW/flyfish/pkg/queue"
	"github.com/sniperHW/flyfish/pkg/raft"
	"github.com/sniperHW/flyfish/pkg/raft/membership"
	snet "github.com/sniperHW/flyfish/server/net"
	"github.com/sniperHW/flyfish/server/slot"
	"net"
	"reflect"
	"sync"
	"sync/atomic"
	"time"
)

type applicationQueue struct {
	q *queue.PriorityQueue
}

type flygate struct {
	service       string
	msgPerSecond  int
	deadlineTimer *time.Timer
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
		return true
	}

	return false
}

func (f *flygateMgr) onHeartBeat(gateService string, msgPerSecond int) {
	var g *flygate
	var ok bool

	if g, ok = f.flygateMap[gateService]; ok {
		g.deadlineTimer.Stop()
	} else {

		if "" == gateService {
			return
		}

		if _, err := net.ResolveTCPAddr("tcp", gateService); nil != err {
			return
		}

		g = &flygate{
			service: gateService,
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
	SlotTransfer    map[int]*TransSlotTransfer
	Meta            Meta
	MetaTransaction *MetaTransaction
	deployment      *deployment
}

func (p *persistenceState) toJson() ([]byte, error) {
	p.Deployment = p.deployment.toDeploymentJson()
	return json.Marshal(&p)
}

func (p *persistenceState) loadFromJson(pd *pd, j []byte) error {
	err := json.Unmarshal(j, p)

	if nil != err {
		return err
	}

	err = p.deployment.loadFromDeploymentJson(&p.Deployment)

	if nil != err {
		return err
	}

	for _, v := range p.deployment.sets {
		if v.markClear {
			pd.markClearSet[v.id] = v
		}
	}

	return nil
}

type pd struct {
	leader          raft.RaftInstanceID
	rn              *raft.RaftInstance
	mutilRaft       *raft.MutilRaft
	mainque         applicationQueue
	udp             *flynet.Udp
	msgHandler      map[reflect.Type]func(*net.UDPAddr, *snet.Message)
	stoponce        int32
	startonce       int32
	wait            sync.WaitGroup
	ready           bool
	flygateMgr      flygateMgr
	config          *Config
	udpService      string
	onBalanceFinish func()
	pState          persistenceState
	storeTask       map[uint64]*storeTask
	markClearSet    map[int]*set
}

func NewPd(id uint16, join bool, config *Config, udpService string, clusterStr string, st membership.Storage) (*pd, error) {

	mainQueue := applicationQueue{
		q: queue.NewPriorityQueue(2, 10000),
	}

	p := &pd{
		mainque:    mainQueue,
		msgHandler: map[reflect.Type]func(*net.UDPAddr, *snet.Message){},
		flygateMgr: flygateMgr{
			flygateMap: map[string]*flygate{},
			mainque:    mainQueue,
		},
		config:       config,
		udpService:   udpService,
		storeTask:    map[uint64]*storeTask{},
		markClearSet: map[int]*set{},
		pState: persistenceState{
			SlotTransfer: map[int]*TransSlotTransfer{},
		},
	}

	p.initMsgHandler()

	peers, err := raft.SplitPeers(clusterStr)

	if nil != err {
		return nil, err
	}

	self, ok := peers[id]

	if !ok {
		return nil, errors.New("cluster not contain self")
	}

	p.mutilRaft = raft.NewMutilRaft()

	p.rn, err = raft.NewInstance(id, 0, join, p.mutilRaft, p.mainque, peers, st, p.config.RaftLogDir, p.config.RaftLogPrefix)

	if nil != err {
		return nil, err
	}

	if err = p.startUdpService(); nil != err {
		p.rn.Stop()
		return nil, err
	}

	GetSugar().Infof("mutilRaft serve on:%s", self.URL)

	go p.mutilRaft.Serve([]string{self.URL})

	p.wait.Add(1)
	go p.serve()

	return p, nil
}

func (q applicationQueue) AppendHighestPriotiryItem(m interface{}) {
	q.q.ForceAppend(1, m)
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
	if nil != p.pState.deployment {
		for _, v := range p.pState.deployment.sets {
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

	for _, v := range p.pState.deployment.sets {
		v.slotOutCount = 0
		v.slotInCount = 0
		for _, vv := range v.stores {
			vv.slotOutCount = 0
			vv.slotInCount = 0
			for _, vvv := range vv.slots.GetOpenBits() {
				if t, ok := p.pState.SlotTransfer[vvv]; ok {
					if vvv == t.StoreTransferIn {
						vv.slotInCount++
						v.slotInCount++
					} else if vvv == t.StoreTransferOut {
						vv.slotOutCount++
						v.slotOutCount++
					}
				}
			}
		}
	}

	var outStore *store

	if len(p.markClearSet) > 0 {
		for _, v := range p.markClearSet {
			if v.getTotalSlotCount()-v.slotOutCount > 0 {
				for _, vv := range v.stores {
					if len(vv.slots.GetOpenBits())-vv.slotOutCount > 0 {
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

	lSets, lMCSets := len(p.pState.deployment.sets), len(p.markClearSet)

	setAverageSlotCount := slot.SlotCount / (lSets - lMCSets)

	storeAverageSlotCount := slot.SlotCount / ((lSets - lMCSets) * StorePerSet)

	if nil == outStore {
		for _, v := range p.pState.deployment.sets {
			if !v.markClear && v.getTotalSlotCount()-v.slotOutCount > setAverageSlotCount+1 {
				for _, vv := range v.stores {
					if len(vv.slots.GetOpenBits())-vv.slotOutCount > storeAverageSlotCount+1 {
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
		for _, v := range p.pState.deployment.sets {
			if !v.markClear && v.getTotalSlotCount()-v.slotInCount < setAverageSlotCount+1 {
				for _, vv := range v.stores {
					if len(vv.slots.GetOpenBits())-vv.slotInCount < storeAverageSlotCount+1 {
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

func (p *pd) isLeader() bool {
	return p.leader == p.rn.ID()
}

func (p *pd) issueProposal(proposal raft.Proposal) {
	p.rn.IssueProposal(proposal)
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
	if nil != p.pState.deployment {
		//重置slotBalance相关的临时数据
		for _, v := range p.pState.deployment.sets {
			p.storeTask = map[uint64]*storeTask{}
			for _, node := range v.nodes {
				for store, state := range node.store {
					if state.Value == FlyKvUnCommit {
						taskID := uint64(node.id)<<32 + uint64(store)
						t := &storeTask{
							node:           node,
							pd:             p,
							store:          store,
							storeStateType: state.Type,
						}
						p.storeTask[taskID] = t
						t.notifyFlyKv()
					}
				}
			}
		}

		p.slotBalance()

		for _, v := range p.pState.SlotTransfer {
			v.notify(p)
		}
	}

	if nil != p.pState.MetaTransaction {
		p.pState.MetaTransaction.notifyStore(p)
	}

}

func (p *pd) onLeaderDownToFollower() {
	for _, v := range p.pState.SlotTransfer {
		v.timer.Stop()
		v.timer = nil
	}

	if nil != p.pState.MetaTransaction && nil != p.pState.MetaTransaction.timer {
		p.pState.MetaTransaction.timer.Stop()
		p.pState.MetaTransaction.timer = nil
	}

	for _, v := range p.storeTask {
		v.timer.Stop()
		v.timer = nil
	}

	p.storeTask = map[uint64]*storeTask{}
}

func (p *pd) Stop() {
	if atomic.CompareAndSwapInt32(&p.stoponce, 0, 1) {
		p.rn.Stop()
		p.wait.Wait()
	}
}

func (p *pd) processCommited(commited raft.Committed) {
	if len(commited.Proposals) > 0 {
		for _, v := range commited.Proposals {
			v.(applyable).apply(p)
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

func (p *pd) serve() {

	go func() {
		defer func() {
			p.udp.Close()
			p.mutilRaft.Stop()
			p.mainque.close()
			p.wait.Done()
		}()
		for {
			_, v := p.mainque.pop()
			switch v.(type) {
			case raft.TransportError:
				GetSugar().Errorf("error for raft transport:%v", v.(raft.TransportError))
			case func():
				v.(func())()
			case raft.Committed:
				p.processCommited(v.(raft.Committed))
			case []raft.LinearizableRead:
			case raft.ProposalConfChange:
			case raft.ConfChange:
				c := v.(raft.ConfChange)
				if c.CCType == raftpb.ConfChangeRemoveNode && c.NodeID == p.rn.ID() {
					p.rn.Stop()
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
					p.onLeaderDownToFollower()
				}
			case *TransSlotTransfer:
				if p.isLeader() {
					if _, ok := p.pState.SlotTransfer[v.(*TransSlotTransfer).Slot]; ok {
						v.(*TransSlotTransfer).notify(p)
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

	persistenceState, err := p.pState.toJson()
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
	return p.pState.loadFromJson(p, reader.GetBytes(int(l)))
}
