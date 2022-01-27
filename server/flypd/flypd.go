package flypd

import (
	"container/list"
	"encoding/json"
	"errors"
	"github.com/gogo/protobuf/proto"
	"github.com/sniperHW/flyfish/db"
	"github.com/sniperHW/flyfish/pkg/bitmap"
	"github.com/sniperHW/flyfish/pkg/buffer"
	"github.com/sniperHW/flyfish/pkg/etcd/pkg/idutil"
	"github.com/sniperHW/flyfish/pkg/etcd/raft/raftpb"
	flynet "github.com/sniperHW/flyfish/pkg/net"
	"github.com/sniperHW/flyfish/pkg/queue"
	"github.com/sniperHW/flyfish/pkg/raft"
	snet "github.com/sniperHW/flyfish/server/net"
	sproto "github.com/sniperHW/flyfish/server/proto"
	"github.com/sniperHW/flyfish/server/slot"
	"net"
	"net/http"
	"os"
	"reflect"
	"sync"
	"sync/atomic"
	"time"
)

type replyer interface {
	reply(*snet.Message)
}

type udpReplyer struct {
	from *net.UDPAddr
	pd   *pd
}

func (ur udpReplyer) reply(resp *snet.Message) {
	ur.pd.udp.SendTo(ur.from, resp)
}

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
	Deployment   DeploymentJson
	SlotTransfer map[int]*TransSlotTransfer
	Meta         db.DbDef
	MetaBytes    []byte
	deployment   deployment
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

type msgHandle struct {
	isConsoleMsg bool
	h            func(replyer, *snet.Message)
}

type msgHandler struct {
	handles     map[reflect.Type]msgHandle
	makeHttpReq map[string]func(*http.Request) (*snet.Message, error)
}

type pd struct {
	leader          uint64
	rn              *raft.RaftInstance
	cluster         int
	mutilRaft       *raft.MutilRaft
	mainque         applicationQueue
	udp             *flynet.Udp
	httpListener    net.Listener
	httpServer      *http.Server
	msgHandler      msgHandler
	closed          int32
	wait            sync.WaitGroup
	flygateMgr      flygateMgr
	config          *Config
	service         string
	onBalanceFinish func()
	pState          persistenceState
	storeTask       map[uint64]*storeTask
	markClearSet    map[int]*set
	metaUpdateQueue *list.List
	RaftIDGen       *idutil.Generator
}

func NewPd(nodeID uint16, cluster int, join bool, config *Config, service string, clusterStr string) (*pd, error) {

	mainQueue := applicationQueue{
		q: queue.NewPriorityQueue(2, 10000),
	}

	p := &pd{
		mainque: mainQueue,
		msgHandler: msgHandler{
			handles:     map[reflect.Type]msgHandle{},
			makeHttpReq: map[string]func(*http.Request) (*snet.Message, error){},
		},
		flygateMgr: flygateMgr{
			flygateMap: map[string]*flygate{},
			mainque:    mainQueue,
		},
		config:       config,
		service:      service,
		cluster:      cluster,
		storeTask:    map[uint64]*storeTask{},
		markClearSet: map[int]*set{},
		pState: persistenceState{
			SlotTransfer: map[int]*TransSlotTransfer{},
			deployment: deployment{
				sets: map[int]*set{},
			},
		},
		metaUpdateQueue: list.New(),
		RaftIDGen:       idutil.NewGenerator(nodeID, time.Now()),
	}

	p.initMsgHandler()

	peers, err := raft.SplitPeers(clusterStr)

	if nil != err {
		return nil, err
	}

	self, ok := peers[nodeID]

	if !ok {
		return nil, errors.New("cluster not contain self")
	}

	p.mutilRaft = raft.NewMutilRaft()

	p.rn, err = raft.NewInstance(nodeID, cluster, join, p.mutilRaft, p.mainque, peers, p.config.RaftLogDir, p.config.RaftLogPrefix)

	if nil != err {
		return nil, err
	}

	if err = p.startUdpService(); nil != err {
		p.rn.Stop()
		return nil, err
	}

	if err = p.startHttpService(); nil != err {
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

func (p *pd) makeReplyFunc(replyer replyer, m *snet.Message, resp proto.Message) func(error) {
	return func(err error) {
		v := reflect.ValueOf(resp).Elem()
		if nil == err {
			v.FieldByName("Ok").SetBool(true)
		} else {
			v.FieldByName("Ok").SetBool(false)
			v.FieldByName("Reason").SetString(err.Error())
		}
		replyer.reply(snet.MakeMessage(m.Context, resp.(proto.Message)))
	}
}

func (p *pd) storeBalance() {
	time.AfterFunc(time.Second*3, func() {
		p.mainque.AppendHighestPriotiryItem(func() {
			if p.isLeader() {
				for _, v := range p.pState.deployment.sets {
					v.storeBalance(p)
				}
			}
			p.storeBalance()
		})
	})
}

func (p *pd) getNode(nodeID int32) *kvnode {
	for _, v := range p.pState.deployment.sets {
		if n, ok := v.nodes[int(nodeID)]; ok {
			return n
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

	var setAverageSlotCount, storeAverageSlotCount int

	if slot.SlotCount%(lSets-lMCSets) == 0 {
		setAverageSlotCount = slot.SlotCount / (lSets - lMCSets)
	} else {
		setAverageSlotCount = (slot.SlotCount / (lSets - lMCSets)) + 1
	}

	if slot.SlotCount%((lSets-lMCSets)*StorePerSet) == 0 {
		storeAverageSlotCount = slot.SlotCount / ((lSets - lMCSets) * StorePerSet)
	} else {
		storeAverageSlotCount = (slot.SlotCount / ((lSets - lMCSets) * StorePerSet)) + 1
	}

	GetSugar().Infof("setAverageSlotCount:%d storeAverageSlotCount:%d", setAverageSlotCount, storeAverageSlotCount)

	if nil == outStore {
		for _, v := range p.pState.deployment.sets {
			if !v.markClear && v.getTotalSlotCount()-v.slotOutCount > setAverageSlotCount {
				for _, vv := range v.stores {
					if len(vv.slots.GetOpenBits())-vv.slotOutCount > storeAverageSlotCount {
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
			if !v.markClear && v.getTotalSlotCount()-v.slotInCount < setAverageSlotCount {
				for _, vv := range v.stores {
					if len(vv.slots.GetOpenBits())-vv.slotInCount < storeAverageSlotCount {
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

/*
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

	for _, j := range storeBitmaps {
		GetSugar().Debugf("onInstallDeployment slots:%v", j.GetOpenBits())
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
				slots: storeBitmaps[i*StorePerSet+j],
				set:   s,
			}

			s.stores[st.id] = st
		}

		for _, vvv := range s.nodes {
			for j := 0; j < StorePerSet; j++ {
				vvv.store[j+1] = &FlyKvStoreState{
					Type:   VoterStore,
					Value:  FlyKvCommited,
					RaftID: d.nextRaftID(),
				}
			}
		}

		d.sets[int(v.SetID)] = s
	}

	return nil
}
*/
func (p *pd) loadInitDeployment() {
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

			var deploymentJson DeploymentJson
			var err error
			if err = json.Unmarshal(b, &deploymentJson); err != nil {
				GetSugar().Errorf("loadInitDeployment err:%v", err)
				return
			}

			if deploymentJson.Version == 0 && len(deploymentJson.Sets) > 0 {
				deploymentJson.Version = 1

				storeCount := len(deploymentJson.Sets) * StorePerSet
				var storeBitmaps []*bitmap.Bitmap

				for i := 0; i < storeCount; i++ {
					storeBitmaps = append(storeBitmaps, bitmap.New(slot.SlotCount))
				}

				jj := 0
				for i := 0; i < slot.SlotCount; i++ {
					storeBitmaps[jj].Set(i)
					jj = (jj + 1) % storeCount
				}

				for i, _ := range deploymentJson.Sets {

					set := &deploymentJson.Sets[i]
					set.Version = 1

					for j := 0; j < StorePerSet; j++ {
						set.Stores = append(set.Stores, StoreJson{
							StoreID: j + 1,
							Slots:   storeBitmaps[i*StorePerSet+j].ToJson(),
						})
					}

					for k, _ := range set.KvNodes {
						set.KvNodes[k].Store = map[int]*FlyKvStoreState{}
						for j := 0; j < StorePerSet; j++ {
							set.KvNodes[k].Store[j+1] = &FlyKvStoreState{
								Type:   VoterStore,
								Value:  FlyKvCommited,
								RaftID: p.RaftIDGen.Next(),
							}
						}
					}
				}

				if err = deploymentJson.check(); nil != err {
					GetSugar().Errorf("loadInitDeployment err:%v", err)
					return
				}

				p.issueProposal(&ProposalInstallDeployment{
					D: deploymentJson,
				})
			}
		}
	}
}

func (p *pd) onBecomeLeader() {

	if len(p.pState.deployment.sets) == 0 {
		p.loadInitDeployment()
	} else {
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

	if p.pState.Meta.Version == 0 {
		p.loadInitMeta()
	}
}

func (p *pd) onLeaderDownToFollower() {
	for _, v := range p.pState.SlotTransfer {
		v.timer.Stop()
		v.timer = nil
	}

	for _, v := range p.storeTask {
		v.timer.Stop()
		v.timer = nil
	}

	p.storeTask = map[uint64]*storeTask{}

	for p.metaUpdateQueue.Len() > 0 {
		op := p.metaUpdateQueue.Remove(p.metaUpdateQueue.Front()).(*metaOpration)
		if nil != op.m.Msg {
			switch op.m.Msg.(type) {
			case *sproto.MetaAddTable:
				op.replyer.reply(snet.MakeMessage(op.m.Context,
					&sproto.MetaAddTableResp{
						Ok:     false,
						Reason: "down to follower",
					}))
			case *sproto.MetaAddFields:
				op.replyer.reply(snet.MakeMessage(op.m.Context,
					&sproto.MetaAddFieldsResp{
						Ok:     false,
						Reason: "down to follower",
					}))
			}
		}
	}
}

func (p *pd) Stop() {
	if atomic.CompareAndSwapInt32(&p.closed, 0, 1) {
		p.rn.Stop()
		p.wait.Wait()
		p.httpListener.Close()
		p.udp.Close()
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

	p.storeBalance()

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
				v.(*ProposalConfChange).reply(nil)
			case raft.ConfChange:
				c := v.(raft.ConfChange)
				if c.CCType == raftpb.ConfChangeRemoveNode && c.NodeID == p.rn.ID() {
					GetSugar().Infof("%x Remove from cluster", p.rn.ID())
					p.rn.Stop()
				}
			case raft.ReplayOK:
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
