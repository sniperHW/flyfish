package raft

import (
	"container/list"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
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

const (
	raftTask_proposal         = 1
	raftTask_confchange       = 2
	raftTask_linearizableread = 3
)

type raftTask struct {
	listE *list.Element
	tt    int
	id    uint64
	other interface{}
	//for LinearizableRead use only
	ptrridx *uint64
	ridx    uint64
}

type raftTaskMgr struct {
	sync.Mutex
	l    *list.List
	dict map[uint64]*raftTask
}

func (this *raftTaskMgr) addToDict(t *raftTask) error {
	this.Lock()
	defer this.Unlock()
	if len(this.dict) > MaxRaftTaskCount {
		return ErrTooManyRequests
	} else {
		this.dict[t.id] = t
		GetSugar().Debugf("raftTaskMgr add %d", t.id)
		return nil
	}
}

func (this *raftTaskMgr) addToDictAndList(t *raftTask) error {
	this.Lock()
	defer this.Unlock()
	if len(this.dict) > MaxRaftTaskCount {
		return ErrTooManyRequests
	} else {
		t.listE = this.l.PushBack(t)
		this.dict[t.id] = t
		GetSugar().Debugf("raftTaskMgr add %d", t.id)
		return nil
	}
}

func (this *raftTaskMgr) remove(t *raftTask) {
	this.Lock()
	defer this.Unlock()
	if nil != t.listE {
		this.l.Remove(t.listE)
	}
	delete(this.dict, t.id)
	GetSugar().Debugf("raftTaskMgr remove %d", t.id)
}

func (this *raftTaskMgr) getAndRemoveByID(id uint64) *raftTask {
	this.Lock()
	defer this.Unlock()
	t, ok := this.dict[id]
	if ok {
		GetSugar().Debugf("raftTaskMgr getAndRemoveByID %d", t.id)
		if nil != t.listE {
			this.l.Remove(t.listE)
		}
		delete(this.dict, t.id)
		return t
	} else {
		return nil
	}
}

func (this *raftTaskMgr) onLeaderDownToFollower() {
	this.Lock()
	this.l = list.New()
	dict := this.dict
	this.dict = map[uint64]*raftTask{}
	this.Unlock()
	for _, v := range dict {
		switch v.other.(type) {
		case []LinearizableRead:
			for _, vv := range v.other.([]LinearizableRead) {
				vv.OnError(ErrLeaderDownToFollower)
			}
		case []Proposal:
			for _, vv := range v.other.([]Proposal) {
				vv.OnError(ErrLeaderDownToFollower)
			}
		}
	}
}

type RaftInstanceID uint32

func (i RaftInstanceID) String() string {
	return strconv.FormatUint(uint64(i), 16)
}

type RaftInstance struct {
	inflightSnapshots   int64
	snapshotIndex       uint64
	appliedIndex        uint64
	lastIndex           uint64 // index of log at start
	snapCount           uint64
	stoponce            int32
	proposePipeline     *queue.ArrayQueue
	readPipeline        *queue.ArrayQueue
	commitC             ApplicationQueue
	waitStop            sync.WaitGroup
	id                  RaftInstanceID // raft instanceID
	nodeID              uint16
	shard               uint16
	join                bool   // node is joining an existing cluster
	waldir              string // path to WAL directory
	snapdir             string // path to snapshot directory
	logdir              string
	confState           raftpb.ConfState
	node                raft.Node
	raftStorage         *raft.MemoryStorage
	wal                 *wal.WAL
	snapshotter         *snap.Snapshotter
	snapshotCh          chan interface{}
	snapshotting        bool //当前是否正在做快照
	transport           *rafthttp.Transport
	stopc               chan struct{} // signals proposal channel closed
	stopping            chan struct{}
	proposalMgr         raftTaskMgr
	linearizableReadMgr raftTaskMgr
	mutilRaft           *MutilRaft
	softState           raft.SoftState
	mb                  *membership.MemberShip
	w                   wait.Wait
	reqIDGen            *idutil.Generator
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

func MakeInstanceID(nodeID uint16, shard uint16) RaftInstanceID {
	return RaftInstanceID(uint32(nodeID)<<16 + uint32(shard))
}

func (r RaftInstanceID) GetNodeID() uint16 {
	return uint16(uint32(r) >> 16)
}

func (r RaftInstanceID) GetShard() uint16 {
	return uint16(uint32(r) & 0x0000FFFF)
}

func (rc *RaftInstance) ID() RaftInstanceID {
	return rc.id
}

func (rc *RaftInstance) isLeader() bool {
	return rc.softState.RaftState == raft.StateLeader
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

			index := binary.BigEndian.Uint64(e.Data[0:8])

			GetSugar().Debugf("entrie %d", index)

			committed = &Committed{
				Data: e.Data[8:],
			}

			if rc.isLeader() {
				if t := rc.proposalMgr.getAndRemoveByID(index); nil != t {
					committed.Proposals = t.other.([]Proposal)
					GetSugar().Debugf("entrie %d with Proposal", index)
				}
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
							ID:       types.ID(cc.NodeID),
							PeerURLs: []string{pc.Url},
						}

						if cc.Type == raftpb.ConfChangeAddNode {
							GetSugar().Infof("%s ConfChangeAddNode %s %s", rc.id.String(), types.ID(cc.NodeID).String(), pc.Url)
						} else {
							m.IsLearner = true
							GetSugar().Infof("%s ConfChangeAddLearnerNode %s %s", rc.id.String(), types.ID(cc.NodeID).String(), pc.Url)
						}
						rc.mb.AddMember(m.ID, m.IsLearner, &m)

						if types.ID(rc.id) != types.ID(cc.NodeID) {
							rc.transport.AddPeer(types.ID(cc.NodeID), []string{pc.Url})
						}

					} else {
						GetSugar().Infof("%s PromoteRaftMember %s", rc.id.String(), types.ID(cc.NodeID).String())
						rc.mb.PromoteMember(types.ID(cc.NodeID))
					}

				case raftpb.ConfChangeRemoveNode:
					GetSugar().Infof("%s ConfChangeRemoveNode %s", rc.id.String(), types.ID(cc.NodeID).String())
					if types.ID(rc.id) != types.ID(cc.NodeID) {
						rc.transport.RemovePeer(types.ID(cc.NodeID))
					}
					rc.mb.RemoveMember(types.ID(cc.NodeID))
				}

				rc.commitC.AppendHighestPriotiryItem(ConfChange{
					CCType:  cc.Type,
					NodeID:  RaftInstanceID(cc.NodeID),
					RaftUrl: pc.Url,
				})

			} else {
				GetSugar().Errorf("%s %s ValidateConfigurationChange IsPromote:%v %s err:%v", rc.id.String(), cc.Type.String(), pc.IsPromote, types.ID(cc.NodeID).String(), err)
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
		rc.appliedIndex = e.Index

		// special nil commit to signal replay has finished
		if e.Index == rc.lastIndex {
			rc.commitC.AppendHighestPriotiryItem(ReplayOK{})
		}
	}
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
			if !rc.snapshotting {
				if atomic.AddInt64(&rc.inflightSnapshots, 1) > MaxInFlightMsgSnap {
					// drop msgSnap if the inflight chan if full.
					atomic.AddInt64(&rc.inflightSnapshots, -1)
				} else {
					//use sendsnap to send the snapshot
					ms[i].Snapshot.Metadata.ConfState = rc.confState
					rc.sendSnapshot(ms[i])
				}
			}
			ms[i].To = 0
		}
	}
	return ms
}

func (rc *RaftInstance) serveChannels() {
	snap, err := rc.raftStorage.Snapshot()
	if err != nil {
		panic(err)
	}
	rc.confState = snap.Metadata.ConfState
	rc.snapshotIndex = snap.Metadata.Index
	rc.appliedIndex = snap.Metadata.Index

	defer func() {
		GetSugar().Infof("%s serveChannels break", rc.id.String())
		rc.wal.Close()
		GetSugar().Infof("%s send RaftStopOK", rc.id.String())
		rc.commitC.AppendHighestPriotiryItem(RaftStopOK{})
	}()

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	rc.runProposePipeline()
	rc.runReadPipeline()

	go func() {
		rc.waitStop.Wait()
		GetSugar().Infof("%s close stopc", rc.id.String())
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
				if !(rc.softState.Lead == rd.SoftState.Lead && rc.softState.RaftState == rd.SoftState.RaftState) {
					oldSoftState := rc.softState
					rc.softState = *rd.SoftState
					if oldSoftState.RaftState == raft.StateLeader {
						if rc.softState.RaftState != raft.StateLeader {
							GetSugar().Infof("(%s) down to follower", rc.id.String())
							rc.proposalMgr.onLeaderDownToFollower()
							rc.linearizableReadMgr.onLeaderDownToFollower()
						}
					} else if rc.softState.RaftState == raft.StateLeader {
						GetSugar().Infof("(%s) becomeLeader", rc.id.String())
					}

					if oldSoftState.Lead != rc.softState.Lead {
						rc.commitC.AppendHighestPriotiryItem(LeaderChange{Leader: RaftInstanceID(rc.softState.Lead)})
					}
				}
			}

			if !raft.IsEmptySnap(rd.Snapshot) {
				rc.saveSnap(rd.Snapshot)
			}

			if err := rc.wal.Save(rd.HardState, rd.Entries); nil != err {
				GetSugar().Fatalf("%s failed to sync Raft snapshot %v", rc.id.String(), err)
			}

			if !raft.IsEmptySnap(rd.Snapshot) {
				rc.raftStorage.ApplySnapshot(rd.Snapshot)
				rc.publishSnapshot(rd.Snapshot)
			}

			rc.raftStorage.Append(rd.Entries)

			rc.transport.Send(rc.processMessages(rd.Messages))

			rc.publishEntries(rc.entriesToApply(rd.CommittedEntries))

			if rc.isLeader() {
				rc.linearizableReadMgr.Lock()
				if len(rd.ReadStates) != 0 {
					rc.processReadStates(rd.ReadStates)
				}
				rc.checkLinearizableRead()
				rc.linearizableReadMgr.Unlock()
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
	GetSugar().Infof("replaying WAL of member %d", rc.id)
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
			GetSugar().Info("ReplayOK 2")
			rc.commitC.AppendHighestPriotiryItem(ReplayOK{})
		}
		return w, nil
	}
}

// isConnectedToQuorumSince checks whether the local member is connected to the
// quorum of the cluster since the given time.
func isConnectedToQuorumSince(transport rafthttp.Transporter, since time.Time, self types.ID, members []*membership.Member) bool {
	GetSugar().Infof("isConnectedToQuorumSince %v", members)
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

func (rc *RaftInstance) MayRemoveMember(id types.ID) error {

	member := rc.mb.Member(id)

	isLearner := (nil != member) && member.IsLearner //rc.mb.IsMemberExist(id) && rc.mb.Member(id).IsLearner
	// no need to check quorum when removing non-voting member
	if isLearner {
		return nil
	}

	// downed member is safe to remove since it's not part of the active quorum
	if t := rc.transport.ActiveSince(id); id != types.ID(rc.ID()) && t.IsZero() {
		return nil
	}

	// protect quorum if some members are down
	m := rc.mb.VotingMembers()
	active := numConnectedSince(rc.transport, time.Now().Add(-HealthInterval), types.ID(rc.ID()), m)
	if (active - 1) < 1+((len(m)-1)/2) {
		GetSugar().Warn(
			"rejecting member remove request; local member has not been connected to all peers, reconfigure breaks active quorum",
			zap.String("local-member-id", rc.ID().String()),
			zap.String("requested-member-remove", id.String()),
			zap.Int("active-peers", active),
			zap.Error(ErrUnhealthy),
		)
		return ErrUnhealthy
	}

	return nil
}

func (rc *RaftInstance) MayAddMember(memb membership.Member) error {
	if /*!isConnectedFullySince*/ !isConnectedToQuorumSince(rc.transport, time.Now().Add(-HealthInterval), types.ID(rc.ID()), rc.mb.VotingMembers()) {
		GetSugar().Warn(
			"rejecting member add request; local member has not been connected to all peers, reconfigure breaks active quorum",
			zap.String("local-member-id", rc.ID().String()),
			zap.String("requested-member-add", fmt.Sprintf("%+v", memb)),
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

type Member struct {
	NodeID    uint16
	URL       string
	IsLearner bool
}

//"NodeID1@URL@learner,NodeID2@URL@"
func SplitPeers(s string) (map[uint16]Member, error) {
	peers := map[uint16]Member{}
	a := strings.Split(s, ",")
	for _, v := range a {
		fields := strings.Split(v, "@")

		if len(fields) != 3 {
			return nil, errors.New("invaild format")
		}

		i, err := strconv.Atoi(fields[0])

		if nil != err {
			return nil, err
		}

		peers[uint16(i)] = Member{
			NodeID:    uint16(i),
			URL:       fields[1],
			IsLearner: fields[2] == "learner",
		}
	}

	return peers, nil
}

func NewInstance(nodeID uint16, shard uint16, join bool, mutilRaft *MutilRaft, commitC ApplicationQueue, peers map[uint16]Member, logdir string, raftLogPrefix string) (*RaftInstance, error) {
	rc := &RaftInstance{
		commitC:    commitC,
		id:         MakeInstanceID(nodeID, shard),
		logdir:     logdir,
		waldir:     fmt.Sprintf("%s/%s-%d-%d-wal", logdir, raftLogPrefix, nodeID, shard),
		snapdir:    fmt.Sprintf("%s/%s-%d-%d-snap", logdir, raftLogPrefix, nodeID, shard),
		snapCount:  DefaultSnapshotCount,
		stopc:      make(chan struct{}),
		stopping:   make(chan struct{}),
		snapshotCh: make(chan interface{}, 1),
		proposalMgr: raftTaskMgr{
			l:    list.New(),
			dict: map[uint64]*raftTask{},
		},
		linearizableReadMgr: raftTaskMgr{
			l:    list.New(),
			dict: map[uint64]*raftTask{},
		},
		mutilRaft:       mutilRaft,
		nodeID:          nodeID,
		shard:           shard,
		proposePipeline: queue.NewArrayQueue(10000),
		readPipeline:    queue.NewArrayQueue(10000),
		w:               wait.New(),
		reqIDGen:        idutil.NewGenerator(nodeID, time.Now()),
	}

	rloger := raftLogger{
		loger: GetLogger().WithOptions(zap.AddCallerSkip(1)),
	}

	rloger.sugar = rloger.loger.Sugar()

	var err error

	if err = fileutil.TouchDirAll(rc.snapdir); err != nil {
		return nil, fmt.Errorf("cannot access snapdir: %v ", err)
	}

	if err = fileutil.TouchDirAll(rc.logdir); err != nil {
		return nil, fmt.Errorf("cannot access logdir: %v ", err)
	}

	rc.snapshotter = snap.New(GetLogger(), rc.snapdir)

	haveWAL := wal.Exist(rc.waldir)

	rc.mb = membership.NewMemberShip(GetLogger(), types.ID(rc.id.GetShard()), types.ID(rc.id.GetNodeID()))

	if rc.wal, err = rc.replayWAL(haveWAL); err != nil {
		return nil, fmt.Errorf("replayWAL : %v ", err)
	}

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
		CheckQuorum:               CheckQuorum,
		PreVote:                   true,
	}

	if haveWAL || join {
		rc.node = raft.RestartNode(c)
	} else {
		rpeers := []raft.Peer{}
		for k, v := range peers {
			cc := membership.ConfChangeContext{
				Url:    v.URL,
				NodeID: uint64(uint64(MakeInstanceID(uint16(k), shard))),
			}

			if v.IsLearner {
				cc.ConfChangeType = raftpb.ConfChangeAddLearnerNode
			} else {
				cc.ConfChangeType = raftpb.ConfChangeAddNode
			}

			context, _ := json.Marshal(cc)

			rpeers = append(rpeers, raft.Peer{ID: uint64(MakeInstanceID(uint16(k), shard)), Context: context})
		}
		rc.node = raft.StartNode(c, rpeers)
	}

	rc.transport = &rafthttp.Transport{
		Logger:      GetLogger(),
		ID:          types.ID(rc.id),
		ClusterID:   types.ID(rc.shard),
		Raft:        rc,
		ServerStats: stats.NewServerStats(types.ID(rc.id).String(), types.ID(rc.id).String()),
		LeaderStats: stats.NewLeaderStats(types.ID(rc.id).String()),
		ErrorC:      make(chan error),
		Snapshotter: rc.snapshotter,
	}

	rc.mutilRaft.addTransport(types.ID(rc.id), rc.transport)

	rc.transport.Start()

	for k, v := range peers {
		if MakeInstanceID(uint16(k), shard) != rc.id {
			rc.transport.AddPeer(types.ID(MakeInstanceID(uint16(k), shard)), []string{v.URL})
		}
	}

	go rc.serveChannels()
	return rc, nil
}
