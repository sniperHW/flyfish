package raft

import (
	"container/list"
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sniperHW/flyfish/pkg/queue"
	"github.com/sniperHW/flyfish/pkg/raft/rafthttp"
	"go.etcd.io/etcd/etcdserver/api/snap"
	stats "go.etcd.io/etcd/etcdserver/api/v2stats"
	"go.etcd.io/etcd/pkg/fileutil"
	"go.etcd.io/etcd/pkg/types"
	"go.etcd.io/etcd/raft"
	"go.etcd.io/etcd/raft/raftpb"
	"go.etcd.io/etcd/wal"
	"go.etcd.io/etcd/wal/walpb"
	"go.uber.org/zap"
)

//应用程序队列必须Append必须是非阻塞的，最好支持优先级
type ApplicationQueue interface {
	//raft投递到应用程序的消息必须用最高优先级,且append不会因超过容量而丢弃
	AppendHighestPriotiryItem(interface{})
}

const (
	raftTask_proposal         = 1
	raftTask_confchange       = 2
	raftTask_linearizableread = 3
)

type raftTask struct {
	lelement *list.Element
	tt       int
	id       uint64
	deadline time.Time
	//for LinearizableRead use only
	ptrridx        *uint64
	ridx           uint64
	other          interface{}
	onTimeout      func()
	onLeaderDemote func()
}

type raftTaskMgr struct {
	sync.Mutex
	l    *list.List
	dict map[uint64]*raftTask
}

func (this *raftTaskMgr) insert(t *raftTask) {
	this.Lock()
	defer this.Unlock()
	t.lelement = this.l.PushBack(t)
	this.dict[t.id] = t
	GetSugar().Debugf("raftTaskMgr insert %d", t.id)
}

func (this *raftTaskMgr) remove(t *raftTask) {
	this.Lock()
	defer this.Unlock()
	this.l.Remove(t.lelement)
	delete(this.dict, t.id)
	GetSugar().Debugf("raftTaskMgr remove %d", t.id)
}

func (this *raftTaskMgr) getAndRemoveByID(id uint64) *raftTask {
	this.Lock()
	defer this.Unlock()
	t, ok := this.dict[id]
	if ok {
		GetSugar().Debugf("raftTaskMgr getAndRemoveByID %d", t.id)
		this.l.Remove(t.lelement)
		delete(this.dict, t.id)
		return t
	} else {
		return nil
	}
}

func (this *raftTaskMgr) runTimeoutCheck(rc *RaftNode) {

	var timeouts []*raftTask
	for {
		select {
		case <-rc.stopc:
			return
		default:
		}
		time.Sleep(time.Millisecond * 10)
		now := time.Now()
		this.Lock()
		for e := this.l.Front(); e != nil; e = this.l.Front() {
			v := e.Value.(*raftTask)
			if v.deadline.IsZero() {
				break
			} else if now.After(v.deadline) {
				if nil != v.onTimeout {
					timeouts = append(timeouts, v)
				}
				this.l.Remove(e)
				delete(this.dict, v.id)
			} else {
				break
			}
		}
		this.Unlock()
		if len(timeouts) > 0 {
			for _, v := range timeouts {
				v.onTimeout()
			}
			timeouts = timeouts[:0]
		}
	}
}

func (this *raftTaskMgr) onLeaderDemote() {
	this.Lock()
	dict := this.dict
	this.l = list.New()
	this.dict = map[uint64]*raftTask{}
	this.Unlock()
	for _, v := range dict {
		v.onLeaderDemote()
	}
}

type RaftNode struct {
	snapshotMerging   int64
	inflightSnapshots int64
	confChangeC       *queue.ArrayQueue
	proposePipeline   *queue.ArrayQueue
	readPipeline      *queue.ArrayQueue
	commitC           ApplicationQueue

	waitStop sync.WaitGroup

	nodeID int
	region int

	id        int // client ID for raft session
	peers     map[int]string
	join      bool   // node is joining an existing cluster
	waldir    string // path to WAL directory
	snapdir   string // path to snapshot directory
	lastIndex uint64 // index of log at start

	confState     raftpb.ConfState
	snapshotIndex uint64
	appliedIndex  uint64

	// raft backing for the commit/error channel
	node         raft.Node
	raftStorage  *raft.MemoryStorage
	wal          *wal.WAL
	snapshotter  *snap.Snapshotter
	snapshotCh   chan interface{}
	snapshotting bool //当前是否正在做快照

	snapCount uint64
	transport *rafthttp.Transport
	stopc     chan struct{} // signals proposal channel closed
	stopping  chan struct{}

	proposalMgr         raftTaskMgr
	confChangeMgr       raftTaskMgr
	linearizableReadMgr raftTaskMgr

	term uint64

	mutilRaft *MutilRaft

	softState raft.SoftState

	idcounter int32
	stoponce  int32

	snapMerge func(...[]byte) ([]byte, error)
}

func readWALNames(dirpath string) []string {
	names, err := fileutil.ReadDir(dirpath)
	if err != nil {
		return nil
	}
	wnames := checkWalNames(names)
	if len(wnames) == 0 {
		return nil
	}
	return wnames
}

func checkWalNames(names []string) []string {
	wnames := make([]string, 0)
	for _, name := range names {
		if _, _, err := parseWALName(name); err != nil {
			continue
		}
		wnames = append(wnames, name)
	}
	return wnames
}

func parseWALName(str string) (seq, index uint64, err error) {
	if !strings.HasSuffix(str, ".wal") {
		return 0, 0, fmt.Errorf("bad wal file")
	}
	_, err = fmt.Sscanf(str, "%016x-%016x.wal", &seq, &index)
	return seq, index, err
}

func searchIndex(names []string, index uint64) (int, bool) {
	for i := len(names) - 1; i >= 0; i-- {
		name := names[i]
		_, curIndex, err := parseWALName(name)
		if err != nil {
			return -1, false
		}
		if index >= curIndex {
			return i, true
		}
	}
	return -1, false
}

func (rc *RaftNode) ID() int {
	return rc.id
}

func (rc *RaftNode) isLeader() bool {
	return rc.softState.RaftState == raft.StateLeader
}

func (rc *RaftNode) genNextIndex() uint64 {
	v := atomic.AddInt32(&rc.idcounter, 1)
	return uint64(rc.id)<<32 + uint64(v)
}

func (rc *RaftNode) removeOldWal(index uint64) {
	names := readWALNames(rc.waldir)
	if names == nil {
		return
	}

	nameIndex, ok := searchIndex(names, index)
	if ok {
		for _, v := range names[:nameIndex] {
			os.Remove(rc.waldir + "/" + v)
			GetSugar().Infof("remove old wal %v", v)
		}
	}
}

func (rc *RaftNode) removeOldSnapAndWal(term uint64, index uint64) {
	go func() {
		filepath.Walk(rc.snapdir,
			func(path string, f os.FileInfo, err error) error {
				if f == nil {
					return err
				}

				if !f.IsDir() && strings.HasSuffix(path, ".snap") {
					filename := strings.TrimLeft(path, rc.snapdir+"/")
					var _term uint64
					var _index uint64

					n, err := fmt.Sscanf(filename, "%016x-%016x.snap", &_term, &_index)
					if nil == err && n == 2 {
						if _term <= term && _index < index {
							os.Remove(path)
							GetSugar().Infof("remove old snap %s", path)
							rc.removeOldWal(_index)
						}
					}
					return nil
				}

				return nil
			})
	}()
}

// openWAL returns a WAL ready for reading.
func (rc *RaftNode) openWAL(snapshot *raftpb.Snapshot) *wal.WAL {
	if !wal.Exist(rc.waldir) {
		if err := os.Mkdir(rc.waldir, 0750); err != nil {
			GetSugar().Fatalf("raftexample: cannot create dir for wal (%v)", err)
		}

		w, err := wal.Create(GetLogger(), rc.waldir, nil)
		if err != nil {
			GetSugar().Fatalf("raftexample: create wal error (%v)", err)
		}
		w.Close()
	}

	walsnap := walpb.Snapshot{}
	if snapshot != nil {
		walsnap.Index, walsnap.Term = snapshot.Metadata.Index, snapshot.Metadata.Term
	}

	GetSugar().Infof("loading WAL at term %d and index %d", walsnap.Term, walsnap.Index)
	w, err := wal.Open(GetLogger(), rc.waldir, walsnap)
	if err != nil {
		GetSugar().Fatalf("raftexample: error loading wal (%v)", err)
	}

	return w
}

// replayWAL replays WAL entries into the raft instance.
func (rc *RaftNode) replayWAL() *wal.WAL {
	GetSugar().Infof("replaying WAL of member %d", rc.id)
	snapshot := rc.loadSnapshot()
	w := rc.openWAL(snapshot)
	_, st, ents, err := w.ReadAll()
	if err != nil {
		GetSugar().Fatalf("raftexample: failed to read WAL (%v)", err)
	} else {
		GetSugar().Infof("ents:%d", len(ents))
	}
	rc.raftStorage = raft.NewMemoryStorage()
	if snapshot != nil {
		rc.raftStorage.ApplySnapshot(*snapshot)
	}
	rc.raftStorage.SetHardState(st)

	if snapshot != nil {
		GetSugar().Info("send replaySnapshot")
		rc.commitC.AppendHighestPriotiryItem(*snapshot)
	}

	// append to storage so raft starts at the right place in log
	rc.raftStorage.Append(ents)
	// send nil once lastIndex is published so client knows commit channel is current
	if len(ents) > 0 {
		rc.lastIndex = ents[len(ents)-1].Index
	} else {
		GetSugar().Info("ReplayOK 2")
		rc.commitC.AppendHighestPriotiryItem(ReplayOK{})
	}
	return w
}

func (rc *RaftNode) entriesToApply(ents []raftpb.Entry) (nents []raftpb.Entry) {
	if len(ents) == 0 {
		return ents
	}
	firstIdx := ents[0].Index
	if firstIdx > rc.appliedIndex+1 {
		GetSugar().Fatalf("first index of committed entry[%d] should <= progress.appliedIndex[%d]+1", firstIdx, rc.appliedIndex)
	}
	if rc.appliedIndex-firstIdx+1 < uint64(len(ents)) {
		nents = ents[rc.appliedIndex-firstIdx+1:]
	}
	return nents
}

// publishEntries writes committed log entries to commit channel and returns
// whether all entries could be published.
func (rc *RaftNode) publishEntries(ents []raftpb.Entry) {
	for i := range ents {

		var committed *Committed

		switch ents[i].Type {
		case raftpb.EntryNormal:
			if len(ents[i].Data) == 0 {
				// ignore empty messages
				break
			}

			index := binary.BigEndian.Uint64(ents[i].Data[0:8])

			GetSugar().Debugf("entrie %d", index)

			committed = &Committed{
				Data: ents[i].Data[8:],
			}

			if rc.isLeader() {
				if t := rc.proposalMgr.getAndRemoveByID(index); nil != t {
					committed.Proposals = t.other.([]Proposal)
					GetSugar().Debugf("entrie %d with Proposal", index)
				}
			}

		case raftpb.EntryConfChange:

			var cc raftpb.ConfChange
			cc.Unmarshal(ents[i].Data)
			rc.confState = *rc.node.ApplyConfChange(cc)

			GetSugar().Infof("%x raftpb.EntryConfChange %d %d %v", rc.id, cc.Type, cc, rc.confState)

			var raftUrl string

			switch cc.Type {
			case raftpb.ConfChangeAddNode, raftpb.ConfChangeAddLearnerNode:
				if len(cc.Context) > 0 {
					raftUrl = string(cc.Context[8:])
					GetSugar().Infof("ConfChangeAddNode %s %s", types.ID(cc.NodeID).String(), raftUrl)
					rc.transport.AddPeer(types.ID(cc.NodeID), []string{raftUrl})
				}
			case raftpb.ConfChangeRemoveNode:
				GetSugar().Infof("ConfChangeRemoveNode %s", types.ID(cc.NodeID).String())
				rc.transport.RemovePeer(types.ID(cc.NodeID))
			}

			if len(cc.Context) > 0 {
				index := binary.BigEndian.Uint64(cc.Context[0:8])
				if rc.isLeader() {
					if t := rc.confChangeMgr.getAndRemoveByID(index); nil != t {
						rc.commitC.AppendHighestPriotiryItem(t.other.(ProposalConfChange))
					}
				}
			}

			rc.commitC.AppendHighestPriotiryItem(ConfChange{
				CCType:  cc.Type,
				NodeID:  int(cc.NodeID >> 16),
				RaftUrl: raftUrl,
			})

		}

		if committed != nil {
			if rc.maybeTriggerSnapshot(ents[i].Index) {
				committed.snapshotNotify = &SnapshotNotify{
					notify: snapshotNotifyst{
						applyIdx: ents[i].Index,
					},
					ch: rc.snapshotCh,
					rc: rc,
				}
			}
			rc.commitC.AppendHighestPriotiryItem(*committed)
		}

		// after commit, update appliedIndex
		rc.appliedIndex = ents[i].Index

		// special nil commit to signal replay has finished
		if ents[i].Index == rc.lastIndex {
			GetSugar().Infof("ReplayOK 1")
			rc.commitC.AppendHighestPriotiryItem(ReplayOK{})
		}
	}
}

func (rc *RaftNode) processMessages(ms []raftpb.Message) []raftpb.Message {
	sentAppResp := false
	for i := len(ms) - 1; i >= 0; i-- {
		if ms[i].Type == raftpb.MsgAppResp {
			if sentAppResp {
				ms[i].To = 0
			} else {
				sentAppResp = true
			}
		}

		if ms[i].Type == raftpb.MsgSnap {
			if !rc.snapshotting && atomic.LoadInt64(&rc.snapshotMerging) == 0 {
				if atomic.AddInt64(&rc.inflightSnapshots, 1) > MaxInFlightMsgSnap {
					// drop msgSnap if the inflight chan if full.
					atomic.AddInt64(&rc.inflightSnapshots, -1)
				} else {
					//use sendsnap to send the snapshot
					ms[i].Snapshot.Metadata.ConfState = rc.confState
					go rc.sendSnapshot(ms[i])
				}
			}
			ms[i].To = 0
		}
	}
	return ms
}

func (rc *RaftNode) serveChannels() {
	snap, err := rc.raftStorage.Snapshot()
	if err != nil {
		panic(err)
	}
	rc.confState = snap.Metadata.ConfState
	rc.snapshotIndex = snap.Metadata.Index
	rc.appliedIndex = snap.Metadata.Index

	defer func() {
		GetSugar().Infof("serveChannels break")
		rc.wal.Close()
		GetSugar().Infof("send RaftStopOK")
		rc.commitC.AppendHighestPriotiryItem(RaftStopOK{})
	}()

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	go rc.linearizableReadMgr.runTimeoutCheck(rc)

	rc.waitStop.Add(3)

	rc.runConfChange()
	rc.runProposePipeline()
	rc.runReadPipeline()

	go func() {
		rc.waitStop.Wait()
		GetSugar().Infof("close stopc")
		close(rc.stopc)
	}()

	// event loop on raft state machine updates
	for {
		select {
		case <-ticker.C:
			rc.node.Tick()

		// store raft entries to wal, then publish over commit channel
		case rd := <-rc.node.Ready():
			if rd.SoftState != nil {
				rc.term = rd.HardState.Term
				if !(rc.softState.Lead == rd.SoftState.Lead && rc.softState.RaftState == rd.SoftState.RaftState) {
					oldSoftState := rc.softState
					rc.softState = *rd.SoftState
					if oldSoftState.RaftState == raft.StateLeader {
						if rc.softState.RaftState != raft.StateLeader {
							rc.proposalMgr.onLeaderDemote()
							rc.confChangeMgr.onLeaderDemote()
							rc.linearizableReadMgr.onLeaderDemote()
						}
					} else if rc.softState.RaftState == raft.StateLeader {
						GetSugar().Infof("becomeLeader id:%x", rc.id)
					}

					if oldSoftState.Lead != rc.softState.Lead {
						rc.commitC.AppendHighestPriotiryItem(LeaderChange{Leader: int(rc.softState.Lead)})
					}
				}
			}

			if !raft.IsEmptySnap(rd.Snapshot) {
				rc.saveSnap(rd.Snapshot)
			}

			if err := rc.wal.Save(rd.HardState, rd.Entries); nil != err {
				GetSugar().Fatalf("failed to sync Raft snapshot %v", err)
			}

			if !raft.IsEmptySnap(rd.Snapshot) {
				rc.raftStorage.ApplySnapshot(rd.Snapshot)
				rc.publishSnapshot(rd.Snapshot)
			}

			rc.raftStorage.Append(rd.Entries)

			rc.transport.Send(rc.processMessages(rd.Messages))

			rc.publishEntries(rc.entriesToApply(rd.CommittedEntries))

			if rc.isLeader() {

				//处理LinearizableRead
				if len(rd.ReadStates) != 0 {
					rc.processReadStates(rd.ReadStates)
				}

				rc.checkLinearizableRead()
			}

			rc.node.Advance()

		case err := <-rc.transport.ErrorC:
			rc.commitC.AppendHighestPriotiryItem(err)
			return
		case <-rc.stopc:
			rc.transport.Stop()
			rc.mutilRaft.removeTransport(types.ID(rc.id))
			return
		case c := <-rc.snapshotCh:
			switch c.(type) {
			case snapshotNotifyst:
				rc.triggerSnapshot(c.(snapshotNotifyst))
			case raftpb.Snapshot:
				rc.onTriggerSnapshotOK(c.(raftpb.Snapshot))
			}
		}
	}
}

func (rc *RaftNode) Stop() {
	if atomic.CompareAndSwapInt32(&rc.stoponce, 0, 1) {
		GetSugar().Infof("RaftNode.Stop()")
		close(rc.stopping)
		rc.confChangeC.Close()
		rc.proposePipeline.Close()
		rc.readPipeline.Close()
	}
}

func (rc *RaftNode) startRaft() {
	if !fileutil.Exist(rc.snapdir) {
		if err := os.Mkdir(rc.snapdir, 0750); err != nil {
			GetSugar().Fatalf("raftexample: cannot create dir for snapshot (%v)", err)
		}
	}
	rc.snapshotter = snap.New(GetLogger(), rc.snapdir)
	oldwal := wal.Exist(rc.waldir)
	rc.wal = rc.replayWAL()

	rpeers := []raft.Peer{}

	for k, _ := range rc.peers {
		id := k<<16 + rc.region
		rpeers = append(rpeers, raft.Peer{ID: uint64(id)})
	}

	rloger := raftLogger{
		loger: GetLogger().WithOptions(zap.AddCallerSkip(1)),
	}
	rloger.sugar = rloger.loger.Sugar()

	c := &raft.Config{
		ID:                        uint64(rc.id),
		ElectionTick:              10,
		HeartbeatTick:             1,
		Storage:                   rc.raftStorage,
		MaxSizePerMsg:             math.MaxUint64, //1024 * 1024,
		MaxInflightMsgs:           256,
		MaxUncommittedEntriesSize: 1 << 30,
		Logger:                    rloger,
		DisableProposalForwarding: true, //禁止非leader转发proposal
		CheckQuorum:               true,
		PreVote:                   true,
	}

	if oldwal || rc.join {
		rc.node = raft.RestartNode(c)
	} else {
		rc.node = raft.StartNode(c, rpeers)
	}

	rc.transport = &rafthttp.Transport{
		Logger:      GetLogger(),
		ID:          types.ID(rc.id),
		ClusterID:   0x10000,
		Raft:        rc,
		ServerStats: stats.NewServerStats("", ""),
		LeaderStats: stats.NewLeaderStats(strconv.Itoa(rc.id)),
		ErrorC:      make(chan error),
		Snapshotter: rc.snapshotter,
	}

	rc.mutilRaft.addTransport(types.ID(rc.id), rc.transport)
	rc.transport.Start()

	for k, v := range rc.peers {
		id := k<<16 + rc.region
		if id != rc.id {
			GetSugar().Infof("AddPeer %s", types.ID(id).String())
			rc.transport.AddPeer(types.ID(id), []string{v})
		}
	}

	go rc.serveChannels()
}

func (rc *RaftNode) Process(ctx context.Context, m raftpb.Message) error {
	return rc.node.Step(ctx, m)
}

func (rc *RaftNode) IsIDRemoved(id uint64) bool {
	//todo member check
	return false
}

func (rc *RaftNode) ReportUnreachable(id uint64) {
	rc.node.ReportUnreachable(id)
}

func (rc *RaftNode) ReportSnapshot(id uint64, status raft.SnapshotStatus) {
	rc.node.ReportSnapshot(id, status)
}

func (rc *RaftNode) IssueLinearizableRead(r LinearizableRead) error {
	return rc.readPipeline.ForceAppend(r)
}

func (rc *RaftNode) IssueProposal(p Proposal) error {
	return rc.proposePipeline.ForceAppend(p)
}

func (rc *RaftNode) IssueConfChange(p ProposalConfChange) error {
	return rc.confChangeC.ForceAppend(p)
}

func NewRaftNode(snapMerge func(...[]byte) ([]byte, error), mutilRaft *MutilRaft, commitC ApplicationQueue, id int, peers map[int]string, join bool, logPath string, raftLogPrefix string) *RaftNode {

	nodeID := id >> 16
	region := id & 0xFFFF

	rc := &RaftNode{
		commitC:    commitC,
		id:         id,
		peers:      peers,
		join:       join,
		waldir:     fmt.Sprintf("%s/%s-%d-%d", logPath, raftLogPrefix, nodeID, region),
		snapdir:    fmt.Sprintf("%s/%s-%d-%d-snap", logPath, raftLogPrefix, nodeID, region),
		snapCount:  DefaultSnapshotCount,
		stopc:      make(chan struct{}),
		stopping:   make(chan struct{}),
		snapshotCh: make(chan interface{}, 1),
		proposalMgr: raftTaskMgr{
			l:    list.New(),
			dict: map[uint64]*raftTask{},
		},
		confChangeMgr: raftTaskMgr{
			l:    list.New(),
			dict: map[uint64]*raftTask{},
		},
		linearizableReadMgr: raftTaskMgr{
			l:    list.New(),
			dict: map[uint64]*raftTask{},
		},
		mutilRaft:       mutilRaft,
		nodeID:          nodeID,
		region:          region,
		confChangeC:     queue.NewArrayQueue(),
		proposePipeline: queue.NewArrayQueue(10000),
		readPipeline:    queue.NewArrayQueue(10000),
		snapMerge:       snapMerge,
	}

	go rc.startRaft()
	return rc
}
