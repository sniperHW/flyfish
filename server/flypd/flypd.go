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

type pd struct {
	id           int
	raftID       int
	leader       int
	rn           *raft.RaftNode
	mutilRaft    *raft.MutilRaft
	mainque      applicationQueue
	udp          *flynet.Udp
	deployment   *deployment
	addingNode   map[int]*AddingNode
	removingNode map[int]*RemovingNode
	slotTransfer map[int]*TransSlotTransfer
	msgHandler   map[reflect.Type]func(*net.UDPAddr, proto.Message)
	stoponce     int32
	startonce    int32
	wait         sync.WaitGroup
	ready        bool
}

func NewPd(udpService string, id int, cluster string) (*pd, error) {
	clusterArray := strings.Split(cluster, ",")

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
		if i == id {
			selfUrl = t[1]
		}
	}

	if selfUrl == "" {
		return nil, errors.New("cluster not contain self")
	}

	mainQueue := applicationQueue{
		q: queue.NewPriorityQueue(2, GetConfig().MainQueueMaxSize),
	}

	mutilRaft := raft.NewMutilRaft()

	rn := raft.NewRaftNode(snapMerge, mutilRaft, mainQueue, (id<<16)+1, peers, false, GetConfig().Log.LogDir, "pd")

	p := &pd{
		id:           id,
		rn:           rn,
		mainque:      mainQueue,
		raftID:       rn.ID(),
		mutilRaft:    mutilRaft,
		msgHandler:   map[reflect.Type]func(*net.UDPAddr, proto.Message){},
		addingNode:   map[int]*AddingNode{},
		removingNode: map[int]*RemovingNode{},
	}

	p.initMsgHandler()

	if err := p.startUdpService(udpService); nil != err {
		rn.Stop()
		return nil, err
	}

	GetSugar().Infof("mutilRaft serve on:%s", selfUrl)

	go p.mutilRaft.Serve(selfUrl)

	p.wait.Add(1)
	go p.serve()

	return p, nil
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

func (p *pd) isLeader() bool {
	return p.leader == p.raftID
}

func (p *pd) issueProposal(proposal raft.Proposal) error {
	return p.rn.IssueProposal(proposal)
}

func (p *pd) startUdpService(udpService string) error {
	udp, err := flynet.NewUdp(udpService, snet.Pack, snet.Unpack)
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
				GetSugar().Infof("got msg")
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

func snapMerge(snaps ...[]byte) ([]byte, error) {
	//pd每次都是全量快照，无需合并，返回最后一个即可
	return snaps[len(snaps)-1], nil
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
				if p.leader == p.raftID {
					p.onBecomeLeader()
				}

				if oldLeader == p.raftID && !p.isLeader() {
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
		p.slotTransfer[int(v.Slot)] = v
	}

	return nil
}
