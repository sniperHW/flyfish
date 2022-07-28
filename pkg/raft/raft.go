package raft

import (
	"container/list"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/sniperHW/flyfish/pkg/buffer"
	"github.com/sniperHW/flyfish/pkg/etcd/etcdserver/api/snap"
	stats "github.com/sniperHW/flyfish/pkg/etcd/etcdserver/api/v2stats"
	"github.com/sniperHW/flyfish/pkg/etcd/pkg/fileutil"
	"github.com/sniperHW/flyfish/pkg/etcd/pkg/idutil"
	"github.com/sniperHW/flyfish/pkg/etcd/pkg/types"
	"github.com/sniperHW/flyfish/pkg/etcd/pkg/wait"
	"github.com/sniperHW/flyfish/pkg/etcd/raft"
	"github.com/sniperHW/flyfish/pkg/etcd/raft/raftpb"
	"github.com/sniperHW/flyfish/pkg/etcd/wal"
	"github.com/sniperHW/flyfish/pkg/etcd/wal/walpb"
	"github.com/sniperHW/flyfish/pkg/queue"
	"github.com/sniperHW/flyfish/pkg/raft/membership"
	"github.com/sniperHW/flyfish/pkg/raft/rafthttp"
	"go.uber.org/zap"
	"io"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

//应用程序队列必须Append必须是非阻塞的，最好支持优先级
type ApplicationQueue interface {
	//raft投递到应用程序的消息必须用最高优先级,且append不会因超过容量而丢弃
	AppendHighestPriotiryItem(interface{})
}

type pendingPropose struct {
	id          uint64
	proposals   []Proposal
	deadline    time.Time
	listElement *list.Element
}

type pendingProposeMgr struct {
	sync.Mutex
	dict map[uint64]*pendingPropose
	l    *list.List
}

func (this *pendingProposeMgr) add(p *pendingPropose) {
	this.Lock()
	defer this.Unlock()
	this.dict[p.id] = p
	p.listElement = this.l.PushBack(p)
}

func (this *pendingProposeMgr) remove(id uint64) *pendingPropose {
	this.Lock()
	defer this.Unlock()
	if p, ok := this.dict[id]; ok {
		delete(this.dict, id)
		this.l.Remove(p.listElement)
		return p
	} else {
		return nil
	}
}

func (this *pendingProposeMgr) checkTimeout(now time.Time) {
	var timeouts []*pendingPropose
	this.Lock()
	for front := this.l.Front(); nil != front; front = this.l.Front() {
		if now.Before(front.Value.(*pendingPropose).deadline) {
			break
		} else {
			e := this.l.Remove(front).(*pendingPropose)
			delete(this.dict, e.id)
			timeouts = append(timeouts, e)
		}
	}
	this.Unlock()
	for _, v := range timeouts {
		for _, vv := range v.proposals {
			vv.OnError(ErrTimeout)
		}
	}
}

const (
	defaultSnapshotCount           uint64 = 10000
	defaultSnapshotCatchUpEntriesN uint64 = 5000
	defaultSnapshotBytes           uint64 = 128 * 1024 * 1024
	defaultMaxBatchCount           int    = 200
)

type RaftInstanceOption struct {
	SnapshotCount           uint64
	SnapshotCatchUpEntriesN uint64
	SnapshotBytes           uint64
	MaxBatchCount           int
	Logdir                  string
	RaftLogPrefix           string
	GetSnapshotData         func(uint64, uint64) ([]byte, error)
	ReadTimeout             time.Duration
}

type notifier struct {
	c   chan struct{}
	err error
}

func newNotifier() *notifier {
	return &notifier{
		c: make(chan struct{}),
	}
}

func (nc *notifier) notify(err error) {
	nc.err = err
	close(nc.c)
}

type RaftInstance struct {
	inflightSnapshots int64
	snapshotIndex     uint64
	appliedIndex      uint64
	lastIndex         uint64 // index of log at start
	stoponce          int32
	proposePipeline   *queue.ArrayQueue
	readPipeline      *queue.ArrayQueue
	commitC           ApplicationQueue
	waitStop          sync.WaitGroup
	id                uint64 // raft instanceID
	lead              uint64
	join              bool   // node is joining an existing cluster
	waldir            string // path to WAL directory
	snapdir           string // path to snapshot directory
	confState         raftpb.ConfState
	node              raft.Node
	raftStorage       *raft.MemoryStorage
	wal               *wal.WAL
	snapshotter       *snap.Snapshotter
	snapshotCh        chan interface{}
	snapshotting      int32 //当前是否正在做快照
	transport         *rafthttp.Transport
	stopc             chan struct{} // signals proposal channel closed
	stopping          chan struct{}
	pendingProposeMgr pendingProposeMgr
	mutilRaft         *MutilRaft
	mb                *membership.MemberShip
	w                 wait.Wait
	reqIDGen          *idutil.Generator
	proposalSize      uint64 //自上次快照以来,proposal总的字节大小
	option            RaftInstanceOption
	applyWait         WaitTime

	// leaderChanged is used to notify the linearizable read loop to drop the old read requests.
	leaderChanged   chan struct{}
	leaderChangedMu sync.RWMutex

	readMu sync.RWMutex
	// read routine notifies etcd server that it waits for reading by sending an empty struct to
	// readwaitC
	readwaitc chan struct{}
	// readNotifier is used to notify the read routine that it can process the request
	// when there is no error
	readNotifier *notifier

	readStateC chan raft.ReadState
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

func (rc *RaftInstance) LeaderChangedNotify() <-chan struct{} {
	rc.leaderChangedMu.RLock()
	defer rc.leaderChangedMu.RUnlock()
	return rc.leaderChanged
}

func (rc *RaftInstance) Snapshotting() bool {
	return atomic.LoadInt32(&rc.snapshotting) == 1
}

func (rc *RaftInstance) GetApplyIndex() uint64 {
	return atomic.LoadUint64(&rc.appliedIndex)
}

func (rc *RaftInstance) ID() uint64 {
	return rc.id
}

func (rc *RaftInstance) isLeader() bool {
	return rc.Lead() == uint64(rc.id)
}

func (rc *RaftInstance) Lead() uint64 {
	return atomic.LoadUint64(&rc.lead)
}

func (rc *RaftInstance) removeOldWal(index uint64) {
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

func (rc *RaftInstance) removeOldSnapAndWal(term uint64, index uint64) {
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
}

func (rc *RaftInstance) entriesToApply(ents []raftpb.Entry) (nents []raftpb.Entry) {
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
func (rc *RaftInstance) publishEntries(ents []raftpb.Entry) {
	for i := range ents {

		var committed *Committed

		e := ents[i]

		switch e.Type {
		case raftpb.EntryNormal:
			if len(e.Data) == 0 {
				// ignore empty messages
				break
			}

			reader := buffer.NewReader(e.Data)
			index := reader.GetUint64()

			committed = &Committed{
				Data: e.Data[8:],
			}

			rc.proposalSize += uint64(len(committed.Data))

			if t := rc.pendingProposeMgr.remove(index); nil != t {
				committed.Proposals = t.proposals
			}

		case raftpb.EntryConfChange:

			var cc raftpb.ConfChange

			cc.Unmarshal(e.Data)

			var err error

			var pc membership.ConfChangeContext
			if err = json.Unmarshal(cc.Context, &pc); nil != err {
				GetSugar().Panicf("Unmarshal proposalConfChange error:%v", err)
			}

			cc.Type = pc.ConfChangeType

			if err = rc.mb.ValidateConfigurationChange(&pc); nil == err {
				rc.confState = *rc.node.ApplyConfChange(cc)
				switch cc.Type {
				case raftpb.ConfChangeAddNode, raftpb.ConfChangeAddLearnerNode:
					if !pc.IsPromote {
						m := membership.Member{
							ID:         types.ID(cc.NodeID),
							PeerURLs:   []string{pc.Url},
							ClientURLs: []string{pc.ClientUrl},
							ProcessID:  pc.ProcessID,
						}

						if cc.Type == raftpb.ConfChangeAddNode {
							GetSugar().Infof("%s ConfChangeAddNode %s %s %s", types.ID(rc.id).String(), types.ID(cc.NodeID).String(), pc.Url, pc.ClientUrl)
						} else {
							m.IsLearner = true
							GetSugar().Infof("%s ConfChangeAddLearnerNode %s %s %s", types.ID(rc.id).String(), types.ID(cc.NodeID).String(), pc.Url, pc.ClientUrl)
						}
						rc.mb.AddMember(m.ID, m.IsLearner, &m)

						if types.ID(rc.id) != types.ID(cc.NodeID) {
							rc.transport.AddPeer(types.ID(cc.NodeID), []string{pc.Url})
						}

					} else {
						GetSugar().Infof("%s PromoteRaftMember %s", types.ID(rc.id).String(), types.ID(cc.NodeID).String())
						rc.mb.PromoteMember(types.ID(cc.NodeID))
					}

				case raftpb.ConfChangeRemoveNode:
					GetSugar().Infof("%s ConfChangeRemoveNode %s", types.ID(rc.id).String(), types.ID(cc.NodeID).String())
					if types.ID(rc.id) != types.ID(cc.NodeID) {
						rc.transport.RemovePeer(types.ID(cc.NodeID))
					}
					rc.mb.RemoveMember(types.ID(cc.NodeID))
				case raftpb.ConfChangeUpdateNode:
					GetSugar().Infof("%s ConfChangeUpdateNode %s", types.ID(rc.id).String(), types.ID(cc.NodeID).String())
					if types.ID(rc.id) != types.ID(cc.NodeID) {
						rc.transport.RemovePeer(types.ID(cc.NodeID))
						rc.transport.AddPeer(types.ID(cc.NodeID), []string{pc.Url})
					}
					rc.mb.UpdateURL(types.ID(cc.NodeID), []string{pc.Url}, []string{pc.ClientUrl})
				}

				rc.commitC.AppendHighestPriotiryItem(ConfChange{
					CCType:  cc.Type,
					NodeID:  cc.NodeID,
					RaftUrl: pc.Url,
				})

			} else {
				GetSugar().Errorf("%s %s ValidateConfigurationChange IsPromote:%v %s err:%v", types.ID(rc.id).String(), cc.Type.String(), pc.IsPromote, types.ID(cc.NodeID).String(), err)
				cc.NodeID = raft.None
				rc.confState = *rc.node.ApplyConfChange(cc)
			}

			if rc.isLeader() {
				rc.w.Trigger(cc.ID, err)
			}
		}

		if committed != nil {
			if rc.maybeTriggerSnapshot(e.Index) {
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
		atomic.StoreUint64(&rc.appliedIndex, e.Index)

		// special nil commit to signal replay has finished
		if e.Index == rc.lastIndex {
			rc.commitC.AppendHighestPriotiryItem(ReplayOK{})
		}
	}

	rc.applyWait.Trigger(atomic.LoadUint64(&rc.appliedIndex))
}

func (rc *RaftInstance) processMessages(ms []raftpb.Message) []raftpb.Message {
	sentAppResp := false
	for i := len(ms) - 1; i >= 0; i-- {
		if rc.IsIDRemoved(ms[i].To) {
			ms[i].To = 0
		}

		if ms[i].Type == raftpb.MsgAppResp {
			if sentAppResp {
				ms[i].To = 0
			} else {
				sentAppResp = true
			}
		}

		if ms[i].Type == raftpb.MsgSnap {
			if !rc.Snapshotting() {
				if atomic.AddInt64(&rc.inflightSnapshots, 1) > MaxInFlightMsgSnap {
					// drop msgSnap if the inflight chan if full.
					atomic.AddInt64(&rc.inflightSnapshots, -1)
				} else {
					//use sendsnap to send the snapshot
					ms[i].Snapshot.Metadata.ConfState = rc.confState
					if err := rc.sendSnapshot(ms[i]); nil != err {
						GetSugar().Errorf("%s sendSnapshot error:%v", types.ID(rc.id).String(), err)
						atomic.AddInt64(&rc.inflightSnapshots, -1)
					}
				}
			}
			ms[i].To = 0
		}
	}
	return ms
}

func (rc *RaftInstance) serveChannels() {

	internalTimeout := time.Second

	snap, err := rc.raftStorage.Snapshot()
	if err != nil {
		panic(err)
	}
	rc.confState = snap.Metadata.ConfState
	rc.snapshotIndex = snap.Metadata.Index
	atomic.StoreUint64(&rc.appliedIndex, snap.Metadata.Index)

	defer func() {
		GetSugar().Infof("%s serveChannels break", types.ID(rc.id).String())
		rc.wal.Close()
		GetSugar().Infof("%s send RaftStopOK", types.ID(rc.id).String())
		rc.commitC.AppendHighestPriotiryItem(RaftStopOK{})
	}()

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	rc.runProposePipeline()
	rc.runReadPipeline()
	go rc.linearizableReadLoop()

	go func() {
		rc.waitStop.Wait()
		GetSugar().Infof("%s close stopc", types.ID(rc.id).String())
		close(rc.stopc)
	}()

	islead := false

	// event loop on raft state machine updates
	for {
		select {
		case <-ticker.C:
			rc.node.Tick()
			rc.pendingProposeMgr.checkTimeout(time.Now())
		// store raft entries to wal, then publish over commit channel
		case rd := <-rc.node.Ready():
			if rd.SoftState != nil {
				newLeader := rd.SoftState.Lead != raft.None && rc.lead != rd.SoftState.Lead
				oldLead := rc.lead

				if oldLead == rc.id && rd.SoftState.Lead != rc.id {
					GetSugar().Infof("(%s) down to follower", types.ID(rc.id).String())
				} else if rd.SoftState.Lead == rc.id && oldLead != rc.id {
					GetSugar().Infof("(%s) becomeLeader", types.ID(rc.id).String())
				}

				if oldLead != rd.SoftState.Lead {
					atomic.StoreUint64(&rc.lead, rd.SoftState.Lead)
					rc.commitC.AppendHighestPriotiryItem(LeaderChange{Leader: rd.SoftState.Lead})
				}

				if newLeader {
					rc.leaderChangedMu.Lock()
					lc := rc.leaderChanged
					rc.leaderChanged = make(chan struct{})
					close(lc)
					rc.leaderChangedMu.Unlock()
				}

				islead = rd.RaftState == raft.StateLeader
			}

			//islead := rc.id == rc.lead

			if len(rd.ReadStates) != 0 {
				select {
				case rc.readStateC <- rd.ReadStates[len(rd.ReadStates)-1]:
				case <-time.After(internalTimeout):
					GetSugar().Warnf("timed out sending read state")
				}
			}

			// the leader can write to its disk in parallel with replicating to the followers and them
			// writing to their disks.
			// For more details, check raft thesis 10.2.1
			if islead {
				rc.transport.Send(rc.processMessages(rd.Messages))
			}

			if !raft.IsEmptySnap(rd.Snapshot) {
				rc.saveSnap(rd.Snapshot)
			}

			if err := rc.wal.Save(rd.HardState, rd.Entries); nil != err {
				GetSugar().Fatalf("%s failed to sync Raft snapshot %v", types.ID(rc.id).String(), err)
			}

			if !raft.IsEmptySnap(rd.Snapshot) {
				rc.raftStorage.ApplySnapshot(rd.Snapshot)
				rc.publishSnapshot(rd.Snapshot)
			}

			rc.raftStorage.Append(rd.Entries)

			rc.publishEntries(rc.entriesToApply(rd.CommittedEntries))

			if !islead {
				rc.transport.Send(rc.processMessages(rd.Messages))
			}

			rc.node.Advance()

		case err := <-rc.transport.ErrorC:
			rc.commitC.AppendHighestPriotiryItem(TransportError(err))
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

func (rc *RaftInstance) Stop() {
	if atomic.CompareAndSwapInt32(&rc.stoponce, 0, 1) {
		close(rc.stopping)
		rc.proposePipeline.Close()
		rc.readPipeline.Close()
	}
}

func (rc *RaftInstance) Process(ctx context.Context, m raftpb.Message) error {
	return rc.node.Step(ctx, m)
}

func (rc *RaftInstance) IsIDRemoved(id uint64) bool {
	return false
}

func (rc *RaftInstance) ReportUnreachable(id uint64) {
	rc.node.ReportUnreachable(id)
}

func (rc *RaftInstance) ReportSnapshot(id uint64, status raft.SnapshotStatus) {
	rc.node.ReportSnapshot(id, status)
}

func (rc *RaftInstance) IssueLinearizableRead(r LinearizableRead) {
	if err := rc.readPipeline.ForceAppend(r); nil != err {
		r.OnError(err)
	}
}

func (rc *RaftInstance) IssueProposal(p Proposal) {
	if err := rc.proposePipeline.ForceAppend(p); nil != err {
		p.OnError(err)
	}
}

func (rc *RaftInstance) IssueConfChange(p ProposalConfChange) {
	rc.proposeConfChange(p)
}

func (rc *RaftInstance) raftStatus() raft.Status {
	return rc.node.Status()
}

// openWAL returns a WAL ready for reading.
func (rc *RaftInstance) openWAL(snapshot *raftpb.Snapshot) (*wal.WAL, error) {
	if !wal.Exist(rc.waldir) {
		if err := os.Mkdir(rc.waldir, 0750); err != nil {
			GetSugar().Fatalf("raftexample: cannot create dir for wal (%v)", err)
			return nil, err
		}

		w, err := wal.Create(GetLogger(), rc.waldir, nil)
		if err != nil {
			GetSugar().Fatalf("raftexample: create wal error (%v)", err)
			return nil, err
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
		return nil, err
	}

	return w, nil
}

// replayWAL replays WAL entries into the raft instance.
func (rc *RaftInstance) replayWAL(haveWAL bool) (*wal.WAL, error) {
	GetSugar().Infof("replaying WAL of member %s", types.ID(rc.id).String())
	if snap, err := rc.loadSnapshot(haveWAL); nil != err {
		return nil, err
	} else {
		var walsnap walpb.Snapshot
		var st raftpb.HardState
		var ents []raftpb.Entry
		if snap != nil {
			walsnap.Index, walsnap.Term = snap.Metadata.Index, snap.Metadata.Term
		}

		var w *wal.WAL
		var err error

		repaired := false
		for {
			if w, err = rc.openWAL(snap); err != nil {
				GetLogger().Fatal("failed to open WAL", zap.Error(err))
			}

			if _, st, ents, err = w.ReadAll(); err != nil {
				w.Close()
				// we can only repair ErrUnexpectedEOF and we never repair twice.
				if repaired || err != io.ErrUnexpectedEOF {
					GetLogger().Fatal("failed to read WAL, cannot be repaired", zap.Error(err))
				}
				if !wal.Repair(GetLogger(), rc.waldir) {
					GetLogger().Fatal("failed to repair WAL", zap.Error(err))
				} else {
					GetLogger().Info("repaired WAL", zap.Error(err))
					repaired = true
				}
				continue
			}
			break
		}

		rc.raftStorage = raft.NewMemoryStorage()
		if snap != nil {
			rc.raftStorage.ApplySnapshot(*snap)
		}

		rc.raftStorage.SetHardState(st)

		if snap != nil {
			rc.recoverMemberShipFromSnapshot(snap)
			rc.commitC.AppendHighestPriotiryItem(*snap)
		}

		// append to storage so raft starts at the right place in log
		rc.raftStorage.Append(ents)
		// send nil once lastIndex is published so client knows commit channel is current
		if len(ents) > 0 {
			rc.lastIndex = ents[len(ents)-1].Index
		} else {
			rc.commitC.AppendHighestPriotiryItem(ReplayOK{})
		}
		return w, nil
	}
}

// isConnectedToQuorumSince checks whether the local member is connected to the
// quorum of the cluster since the given time.
func isConnectedToQuorumSince(transport rafthttp.Transporter, since time.Time, self types.ID, members []*membership.Member) bool {
	return numConnectedSince(transport, since, self, members) >= (len(members)/2)+1
}

// isConnectedSince checks whether the local member is connected to the
// remote member since the given time.
func isConnectedSince(transport rafthttp.Transporter, since time.Time, remote types.ID) bool {
	t := transport.ActiveSince(remote)
	return !t.IsZero() && t.Before(since)
}

// isConnectedFullySince checks whether the local member is connected to all
// members in the cluster since the given time.
func isConnectedFullySince(transport rafthttp.Transporter, since time.Time, self types.ID, members []*membership.Member) bool {
	return numConnectedSince(transport, since, self, members) == len(members)
}

// numConnectedSince counts how many members are connected to the local member
// since the given time.
func numConnectedSince(transport rafthttp.Transporter, since time.Time, self types.ID, members []*membership.Member) int {
	connectedNum := 0
	for _, m := range members {
		if m.ID == self || isConnectedSince(transport, since, m.ID) {
			connectedNum++
		}
	}
	return connectedNum
}

func (rc *RaftInstance) GetRaftCluster() string {
	var tmp []string
	for _, v := range rc.mb.Members() {
		if v.IsLearner {
			tmp = append(tmp, fmt.Sprintf("%d@%d@%s@%s@learner", v.ProcessID, uint64(v.ID), v.PeerURLs[0], v.ClientURLs[0]))
		} else {
			tmp = append(tmp, fmt.Sprintf("%d@%d@%s@%s@voter", v.ProcessID, uint64(v.ID), v.PeerURLs[0], v.ClientURLs[0]))
		}
	}

	return strings.Join(tmp, ",")

}

func (rc *RaftInstance) MayRemoveMember(id types.ID) error {

	member := rc.mb.Member(id)

	if member == nil {
		return membership.ErrIDNotFound
	}

	// no need to check quorum when removing non-voting member
	if member.IsLearner {
		return nil
	}

	// downed member is safe to remove since it's not part of the active quorum
	if t := rc.transport.ActiveSince(id); id != types.ID(rc.ID()) && t.IsZero() {
		return nil
	}

	// protect quorum if some members are down
	if !isConnectedToQuorumSince(rc.transport, time.Now().Add(-HealthInterval), types.ID(rc.ID()), rc.mb.VotingMembers()) {
		GetSugar().Warn(
			"rejecting member remove request; local member has not been connected to all peers, reconfigure breaks active quorum",
			zap.String("local-member-id", types.ID(rc.id).String()),
			zap.String("requested-member-remove", id.String()),
			zap.Error(ErrUnhealthy),
		)
		return ErrUnhealthy
	}

	return nil
}

func (rc *RaftInstance) MayAddMember(id types.ID) error {
	if nil != rc.mb.Member(id) {
		return membership.ErrIDExists
	}

	if !isConnectedFullySince(rc.transport, time.Now().Add(-HealthInterval), types.ID(rc.ID()), rc.mb.VotingMembers()) {
		GetSugar().Warn(
			"rejecting member add request; local member has not been connected to all peers, reconfigure breaks active quorum",
			zap.String("local-member-id", types.ID(rc.id).String()),
			zap.String("requested-member-add", id.String()),
			zap.Error(ErrUnhealthy),
		)
		return ErrUnhealthy
	}

	return nil
}

func (rc *RaftInstance) IsLearnerReady(id uint64) error {
	rs := rc.raftStatus()

	// leader's raftStatus.Progress is not nil
	if rs.Progress == nil {
		return ErrNotLeader
	}

	var learnerMatch uint64
	isFound := false
	leaderID := rs.ID
	for memberID, progress := range rs.Progress {
		if id == memberID {
			// check its status
			learnerMatch = progress.Match
			isFound = true
			break
		}
	}

	if isFound {
		leaderMatch := rs.Progress[leaderID].Match
		// the learner's Match not caught up with leader yet
		if float64(learnerMatch) < float64(leaderMatch)*ReadyPercent {
			return ErrLearnerNotReady
		}

		return nil

	} else {
		return membership.ErrIDNotFound
	}
}

func (rc *RaftInstance) GetMemberProgress(id uint64) (error, float64) {
	rs := rc.raftStatus()

	// leader's raftStatus.Progress is not nil
	if rs.Progress == nil {
		return ErrNotLeader, 0.0
	}

	var learnerMatch uint64
	isFound := false
	leaderID := rs.ID
	for memberID, progress := range rs.Progress {
		if id == memberID {
			// check its status
			learnerMatch = progress.Match
			isFound = true
			break
		}
	}

	if isFound {
		leaderMatch := rs.Progress[leaderID].Match
		return nil, float64(learnerMatch) / float64(leaderMatch)
	} else {
		return membership.ErrIDNotFound, 0.0
	}

}

func (rc *RaftInstance) Members() (membs []Member) {
	for _, v := range rc.mb.Members() {
		membs = append(membs, Member{
			ProcessID: v.ProcessID,
			ID:        uint64(v.ID),
			URL:       v.PeerURLs[0],
			ClientURL: v.ClientURLs[0],
			IsLearner: v.IsLearner,
		})
	}
	return
}

type Member struct {
	ProcessID uint16
	ID        uint64
	URL       string
	ClientURL string
	IsLearner bool
}

//"ProcessID1@InstanceID1@URL@ClientURL@learner,ProcessID2@InstanceID2@URL@ClientURL@"
func SplitPeers(s string) (map[uint16]Member, error) {
	peers := map[uint16]Member{}
	a := strings.Split(s, ",")
	for _, v := range a {
		fields := strings.Split(v, "@")

		if len(fields) != 5 {
			return nil, errors.New("invaild format")
		}

		processID, err := strconv.ParseUint(fields[0], 10, 16)

		if nil != err {
			return nil, err
		}

		i, err := strconv.ParseUint(fields[1], 10, 64)

		if nil != err {
			return nil, err
		}

		if _, ok := peers[uint16(processID)]; ok {
			return nil, fmt.Errorf("duplicate processID:%d", processID)
		}

		peers[uint16(processID)] = Member{
			ProcessID: uint16(processID),
			ID:        i,
			URL:       fields[2],
			ClientURL: fields[3],
			IsLearner: fields[4] == "learner",
		}
	}

	return peers, nil
}

func NewInstance(processID uint16, cluster int, join bool, mutilRaft *MutilRaft, commitC ApplicationQueue, peers map[uint16]Member, option RaftInstanceOption) (*RaftInstance, error) {
	mbSelf, ok := peers[processID]
	if !ok {
		return nil, fmt.Errorf("peers not contain self")
	}

	if option.SnapshotCount == 0 {
		option.SnapshotCount = defaultSnapshotCount
	}

	if option.SnapshotCatchUpEntriesN == 0 {
		option.SnapshotCatchUpEntriesN = defaultSnapshotCatchUpEntriesN
	}

	if option.SnapshotBytes == 0 {
		option.SnapshotBytes = defaultSnapshotBytes
	}

	if option.MaxBatchCount <= 0 {
		option.MaxBatchCount = defaultMaxBatchCount
	}

	if option.ReadTimeout <= 0 {
		option.ReadTimeout = time.Second * 2
	}

	rc := &RaftInstance{
		commitC:    commitC,
		id:         mbSelf.ID,
		option:     option,
		waldir:     fmt.Sprintf("%s/%s-%d-%d-%x-wal", option.Logdir, option.RaftLogPrefix, processID, cluster, mbSelf.ID),
		snapdir:    fmt.Sprintf("%s/%s-%d-%d-%x-snap", option.Logdir, option.RaftLogPrefix, processID, cluster, mbSelf.ID),
		stopc:      make(chan struct{}),
		stopping:   make(chan struct{}),
		snapshotCh: make(chan interface{}, 1),
		pendingProposeMgr: pendingProposeMgr{
			dict: map[uint64]*pendingPropose{},
			l:    list.New(),
		},
		mutilRaft:       mutilRaft,
		proposePipeline: queue.NewArrayQueue(1024),
		readPipeline:    queue.NewArrayQueue(1024),
		w:               wait.New(),
		reqIDGen:        idutil.NewGenerator(processID, time.Now()),
		applyWait:       NewTimeList(),
		readwaitc:       make(chan struct{}, 1),
		readNotifier:    newNotifier(),
		leaderChanged:   make(chan struct{}),
		readStateC:      make(chan raft.ReadState, 1),
	}

	rloger := raftLogger{
		loger: GetLogger().WithOptions(zap.AddCallerSkip(1)),
	}

	rloger.sugar = rloger.loger.Sugar()

	var err error

	if err = fileutil.TouchDirAll(rc.snapdir); err != nil {
		return nil, fmt.Errorf("cannot access snapdir: %v ", err)
	}

	if err = fileutil.TouchDirAll(option.Logdir); err != nil {
		return nil, fmt.Errorf("cannot access logdir: %v ", err)
	}

	rc.snapshotter = snap.New(GetLogger(), rc.snapdir)

	haveWAL := wal.Exist(rc.waldir)

	rc.mb = membership.NewMemberShip(GetLogger(), types.ID(rc.id), types.ID(cluster))

	if rc.wal, err = rc.replayWAL(haveWAL); err != nil {
		return nil, fmt.Errorf("replayWAL : %v ", err)
	}

	c := &raft.Config{
		ID:                        rc.id,
		ElectionTick:              10,
		HeartbeatTick:             1,
		Storage:                   rc.raftStorage,
		MaxSizePerMsg:             math.MaxUint64, //1024 * 1024,
		MaxInflightMsgs:           256,
		MaxUncommittedEntriesSize: 1 << 30,
		Logger:                    rloger,
		DisableProposalForwarding: true, //禁止非leader转发proposal
		CheckQuorum:               checkQuorum,
		PreVote:                   true,
	}

	if haveWAL || join {
		rc.node = raft.RestartNode(c)
	} else {
		rpeers := []raft.Peer{}
		for _, v := range peers {
			cc := membership.ConfChangeContext{
				Url:       v.URL,
				ClientUrl: v.ClientURL,
				NodeID:    v.ID,
				ProcessID: v.ProcessID,
			}

			if v.IsLearner {
				cc.ConfChangeType = raftpb.ConfChangeAddLearnerNode
			} else {
				cc.ConfChangeType = raftpb.ConfChangeAddNode
			}

			context, _ := json.Marshal(cc)

			rpeers = append(rpeers, raft.Peer{ID: v.ID, Context: context})
		}
		rc.node = raft.StartNode(c, rpeers)
	}

	rc.transport = &rafthttp.Transport{
		Logger:      GetLogger(),
		ID:          types.ID(rc.id),
		ClusterID:   types.ID(cluster),
		Raft:        rc,
		ServerStats: stats.NewServerStats(types.ID(mbSelf.ProcessID).String(), types.ID(rc.id).String()),
		LeaderStats: stats.NewLeaderStats(types.ID(rc.id).String()),
		ErrorC:      make(chan error),
		Snapshotter: rc.snapshotter,
	}

	rc.mutilRaft.addTransport(types.ID(rc.id), rc.transport)

	rc.transport.Start()

	mb := rc.mb.Members()

	if len(mb) == 0 {
		for _, v := range peers {
			if v.ID != rc.id {
				rc.transport.AddPeer(types.ID(v.ID), []string{v.URL})
			}
		}
	} else {
		for _, v := range mb {
			if uint64(v.ID) != rc.id {
				rc.transport.AddPeer(types.ID(v.ID), v.PeerURLs)
			}
		}
	}

	go rc.serveChannels()
	return rc, nil
}
