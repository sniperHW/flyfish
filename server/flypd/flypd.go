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
	replyed int32
	from    *net.UDPAddr
	pd      *pd
}

func (ur *udpReplyer) reply(resp *snet.Message) {
	if atomic.CompareAndSwapInt32(&ur.replyed, 0, 1) {
		ur.pd.udp.SendTo(ur.from, resp)
	}
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
	GetSugar().Infof("%s HeartBeat timeout remove", gateService)
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
	SlotTransferMgr SlotTransferMgr
	Meta            db.DbDef
	MetaBytes       []byte
	deployment      deployment
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
	httpServer      *http.Server
	msgHandler      msgHandler
	closed          int32
	wait            sync.WaitGroup
	flygateMgr      flygateMgr
	config          *Config
	service         string
	pState          persistenceState
	storeTask       map[uint64]*storeTask
	metaUpdateQueue *list.List
	RaftIDGen       *idutil.Generator
	pendingNodes    map[int]*kvnode
}

func NewPd(nodeID uint16, cluster int, join bool, config *Config, clusterStr string) (*pd, error) {

	peers, err := raft.SplitPeers(clusterStr)

	if nil != err {
		return nil, err
	}

	self, ok := peers[nodeID]

	if !ok {
		return nil, errors.New("cluster not contain self")
	}

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
		config:    config,
		service:   self.ClientURL,
		cluster:   cluster,
		storeTask: map[uint64]*storeTask{},
		pState: persistenceState{
			SlotTransferMgr: SlotTransferMgr{
				Transactions: map[int]*TransSlotTransfer{},
				Plan:         map[int]*SlotTransferPlan{},
				FreeSlots:    map[int]bool{},
			},
			deployment: deployment{
				sets: map[int]*set{},
			},
		},
		metaUpdateQueue: list.New(),
		RaftIDGen:       idutil.NewGenerator(nodeID, time.Now()),
		pendingNodes:    map[int]*kvnode{},
	}

	p.initMsgHandler()

	p.mutilRaft = raft.NewMutilRaft()

	p.rn, err = raft.NewInstance(nodeID, cluster, join, p.mutilRaft, p.mainque, peers, raft.RaftInstanceOption{
		Logdir:        p.config.RaftLogDir,
		RaftLogPrefix: p.config.RaftLogPrefix,
	})

	if nil != err {
		return nil, err
	}

	if err = p.startUdpService(); nil != err {
		p.rn.Stop()
		return nil, err
	}

	p.startHttpService()

	GetSugar().Infof("mutilRaft serve on:%s", self.URL)

	go p.mutilRaft.Serve(self.URL)

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
	time.AfterFunc(time.Second*1, func() {
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

func (p *pd) isLeader() bool {
	return atomic.LoadUint64(&p.leader) == p.rn.ID()
}

func (p *pd) issueProposal(proposal raft.Proposal) {
	p.rn.IssueProposal(proposal)
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
		} else {
			GetSugar().Errorf("loadInitDeployment err:%v", err)
		}
	}
}

func (p *pd) onBecomeLeader() {
	p.issueProposal(&ProposalNop{})
}

func (p *pd) onLeaderDownToFollower() {
	for _, v := range p.pState.SlotTransferMgr.Transactions {
		if nil != v.timer {
			v.timer.Stop()
			v.timer = nil
		}
	}

	for _, v := range p.storeTask {
		v.mtx.Lock()
		if nil != v.timer {
			v.timer.Stop()
			v.timer = nil
		}
		v.mtx.Unlock()
	}

	p.storeTask = map[uint64]*storeTask{}

	p.pendingNodes = map[int]*kvnode{}

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
		p.httpServer.Close()
		p.udp.Close()
	}
}

func (p *pd) processCommited(commited raft.Committed) {
	if len(commited.Proposals) > 0 {
		for _, v := range commited.Proposals {
			v.(applyable).apply(p)
		}
	} else {
		reader := buffer.NewReader(commited.Data)
		var r applyable
		for !reader.IsOver() {

			proposalType, j, err := func() (proposalType byte, j []byte, err error) {
				var l int32
				if proposalType, err = reader.CheckGetByte(); nil != err {
					return
				}

				if l, err = reader.CheckGetInt32(); nil != err {
					return
				}

				j, err = reader.CheckGetBytes(int(l))

				return
			}()

			if nil == err {
				unmarshal := func(rr interface{}) (err error) {
					if err = json.Unmarshal(j, rr); nil == err {
						r = rr.(applyable)
					}
					return
				}

				switch int(proposalType) {
				case proposalInstallDeployment:
					err = unmarshal(&ProposalInstallDeployment{})
				case proposalAddNode:
					err = unmarshal(&ProposalAddNode{})
				case proposalRemNode:
					err = unmarshal(&ProposalRemNode{})
				case proposalSlotTransOutOk:
					err = unmarshal(&ProposalSlotTransOutOk{})
				case proposalSlotTransInOk:
					err = unmarshal(&ProposalSlotTransInOk{})
				case proposalAddSet:
					err = unmarshal(&ProposalAddSet{})
				case proposalRemSet:
					err = unmarshal(&ProposalRemSet{})
				case proposalSetMarkClear:
					err = unmarshal(&ProposalSetMarkClear{})
				case proposalInitMeta:
					err = unmarshal(&ProposalInitMeta{})
				case proposalUpdateMeta:
					err = unmarshal(&ProposalUpdateMeta{})
				case proposalFlyKvCommited:
					err = unmarshal(&ProposalFlyKvCommited{})
				/*case proposalAddLearnerStoreToNode:
					err = unmarshal(&ProposalAddLearnerStoreToNode{})
				case proposalPromoteLearnerStore:
					err = unmarshal(&ProposalPromoteLearnerStore{})
				case proposalRemoveNodeStore:
					err = unmarshal(&ProposalRemoveNodeStore{})
				*/
				case proposalNop:
					err = unmarshal(&ProposalNop{})
				default:
					err = errors.New("invaild proposal type")
				}
			}

			if nil == err {
				r.apply(p)
			} else {
				GetSugar().Panic(err)
			}

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

func (p *pd) processPendingNodes() {
	if p.isLeader() {
		if len(p.pendingNodes) > 0 {
			dict := map[uint64]bool{}
			for _, v := range p.storeTask {
				dict[uint64(v.node.id)<<32+uint64(v.store)] = true
			}

			for _, v := range p.pendingNodes {
				for k, vv := range v.store {
					if vv.Type == VoterStore && vv.Value == FlyKvUnCommit && !dict[uint64(v.id)<<32+uint64(k)] {
						p.startStoreNotifyTask(v, k, vv.RaftID, VoterStore)
					}
				}
			}
		}
	}

	time.AfterFunc(time.Second, func() {
		p.mainque.AppendHighestPriotiryItem(p.processPendingNodes)
	})
}

func (p *pd) serve() {

	p.storeBalance()

	go func() {
		defer func() {
			p.udp.Close()
			p.mutilRaft.Stop()
			p.mainque.close()
			p.wait.Done()
			GetSugar().Infof("pd serve break")
		}()

		time.AfterFunc(time.Second, func() {
			p.mainque.AppendHighestPriotiryItem(p.processPendingNodes)
		})

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
				atomic.StoreUint64(&p.leader, v.(raft.LeaderChange).Leader)
				if p.leader == p.rn.ID() {
					p.onBecomeLeader()
				}

				if oldLeader == p.rn.ID() && p.leader != p.rn.ID() {
					p.onLeaderDownToFollower()
				}
			case *TransSlotTransfer:
				if p.leader == p.rn.ID() {
					if _, ok := p.pState.SlotTransferMgr.Transactions[v.(*TransSlotTransfer).Slot]; ok {
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
