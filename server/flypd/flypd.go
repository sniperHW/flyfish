package flypd

import (
	"container/list"
	"encoding/json"
	"errors"
	"github.com/gogo/protobuf/proto"
	"github.com/sniperHW/flyfish/pkg/bitmap"
	"github.com/sniperHW/flyfish/pkg/buffer"
	"github.com/sniperHW/flyfish/pkg/etcd/pkg/idutil"
	"github.com/sniperHW/flyfish/pkg/etcd/raft/raftpb"
	flynet "github.com/sniperHW/flyfish/pkg/net"
	"github.com/sniperHW/flyfish/pkg/queue"
	"github.com/sniperHW/flyfish/pkg/raft"
	snet "github.com/sniperHW/flyfish/server/net"
	"net"
	"net/http"
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

type msgHandle struct {
	isConsoleMsg bool
	h            func(replyer, *snet.Message)
}

type msgHandler struct {
	handles     map[reflect.Type]msgHandle
	makeHttpReq map[string]func(*http.Request) (*snet.Message, error)
}

type ProposalConfChange struct {
	confChangeType raftpb.ConfChangeType
	url            string //for add
	clientUrl      string
	nodeID         uint64
	processID      uint16
	reply          func(error)
}

func (this *ProposalConfChange) GetType() raftpb.ConfChangeType {
	return this.confChangeType
}

func (this *ProposalConfChange) GetURL() string {
	return this.url
}

func (this *ProposalConfChange) GetClientURL() string {
	return this.clientUrl
}

func (this *ProposalConfChange) GetNodeID() uint64 {
	return this.nodeID
}

func (this *ProposalConfChange) GetProcessID() uint16 {
	return this.processID
}

func (this *ProposalConfChange) IsPromote() bool {
	return false
}

func (this *ProposalConfChange) OnError(err error) {
	this.reply(err)
}

type endpoint struct {
	service       string
	metaVersion   int64
	deadlineTimer *time.Timer
}

type endpointMgr struct {
	dict map[string]*endpoint
}

func (f *endpointMgr) getEndpoints() (ret []string) {
	for k, _ := range f.dict {
		ret = append(ret, k)
	}
	return
}

func (f *endpointMgr) onTimeout(service string, t *time.Timer) {
	if v, ok := f.dict[service]; ok && v.deadlineTimer == t {
		delete(f.dict, service)
	}
}

func (f *endpointMgr) onHeartBeat(p *pd, service string, metaVersion int64) {
	var e *endpoint
	var ok bool

	if e, ok = f.dict[service]; ok {
		e.deadlineTimer.Stop()
	} else {

		if "" == service {
			return
		}

		if _, err := net.ResolveTCPAddr("tcp", service); nil != err {
			return
		}

		e = &endpoint{
			service: service,
		}
		f.dict[service] = e
	}

	var deadlineTimer *time.Timer

	deadlineTimer = time.AfterFunc(time.Second*10, func() {
		p.mainque.AppendHighestPriotiryItem(func() {
			f.onTimeout(service, deadlineTimer)
		})
	})

	e.deadlineTimer = deadlineTimer
	e.metaVersion = metaVersion
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
	config          *Config
	service         string
	raftIDGen       *idutil.Generator
	flygateMgr      endpointMgr
	flysqlMgr       endpointMgr
	Deployment      Deployment
	DbMetaMgr       DbMetaMgr
	SlotTransferMgr SlotTransferMgr
	MaxSequenceID   int64
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

func (p *pd) getNode(nodeID int32) (*Set, *KvNode) {
	for _, set := range p.Deployment.Sets {
		if node, ok := set.Nodes[int(nodeID)]; ok {
			return set, node
		}
	}
	return nil, nil
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

func (p *pd) isLeader() bool {
	return atomic.LoadUint64(&p.leader) == p.rn.ID()
}

func (p *pd) issueProposal(proposal raft.Proposal) {
	p.rn.IssueProposal(proposal)
}

func (p *pd) onMsg(replyer replyer, msg *snet.Message) {
	if h, ok := p.msgHandler.handles[reflect.TypeOf(msg.Msg)]; ok {
		if p.config.DisableUdpConsole && h.isConsoleMsg {
			//禁止udp console接口，如果请求来自udp全部
			if _, ok := replyer.(*udpReplyer); ok {
				return
			}
		}
		h.h(replyer, msg)
	}
}

func (p *pd) onBecomeLeader() {
	p.issueProposal(&ProposalNop{})
}

func (p *pd) storeBalance() {
	time.AfterFunc(time.Second*1, func() {
		p.mainque.AppendHighestPriotiryItem(func() {
			if p.isLeader() {
				for _, set := range p.Deployment.Sets {
					set.storeBalance(p)
				}
			}
			p.storeBalance()
		})
	})
}

func (p *pd) onLeaderDownToFollower() {
	for _, v := range p.SlotTransferMgr.Transactions {
		if nil != v.timer {
			v.timer.Stop()
			v.timer = nil
		}
	}

	p.DbMetaMgr.clearOpQueue()
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
				case proposalNop:
					err = unmarshal(&ProposalNop{})
				case proposalOrderSequenceID:
					err = unmarshal(&ProposalOrderSequenceID{})
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

func (p *pd) serve() {
	go func() {
		defer func() {
			p.udp.Close()
			p.mutilRaft.Stop()
			p.mainque.close()
			p.wait.Done()
			GetSugar().Infof("pd serve break")
		}()

		time.AfterFunc(time.Second, func() {
			p.mainque.AppendHighestPriotiryItem(p.processNodeNotify)
		})

		p.storeBalance()

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
					if _, ok := p.SlotTransferMgr.Transactions[v.(*TransSlotTransfer).Slot]; ok {
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
	snapshot, err := json.Marshal(p)
	if nil != err {
		return nil, err
	}
	b := make([]byte, 0, 4+len(snapshot))

	b = buffer.AppendInt32(b, int32(len(snapshot)))
	b = buffer.AppendBytes(b, snapshot)
	return b, nil
}

func (p *pd) recoverFromSnapshot(b []byte) error {
	reader := buffer.NewReader(b)
	if err := json.Unmarshal(reader.GetBytes(int(reader.GetInt32())), p); nil != err {
		for _, set := range p.Deployment.Sets {
			for _, store := range set.Stores {
				store.slots, _ = bitmap.CreateFromJson(store.Slots)
			}
		}
		return err
	} else {
		return nil
	}
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
		flygateMgr: endpointMgr{
			dict: map[string]*endpoint{},
		},
		flysqlMgr: endpointMgr{
			dict: map[string]*endpoint{},
		},
		config:    config,
		service:   self.ClientURL,
		cluster:   cluster,
		raftIDGen: idutil.NewGenerator(nodeID, time.Now()),
		Deployment: Deployment{
			Sets: map[int]*Set{},
		},
		DbMetaMgr: DbMetaMgr{
			opQueue: list.New(),
		},
		SlotTransferMgr: SlotTransferMgr{
			Transactions: map[int]*TransSlotTransfer{},
			Plan:         map[int]*SlotTransferPlan{},
			FreeSlots:    map[int]bool{},
		},
		MaxSequenceID: 1,
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
