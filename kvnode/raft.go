// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package kvnode

import (
	"container/list"
	"context"
	"encoding/binary"
	"fmt"
	"github.com/sniperHW/flyfish/conf"
	"github.com/sniperHW/flyfish/errcode"
	"github.com/sniperHW/flyfish/rafthttp"
	"github.com/sniperHW/flyfish/util/fixedarray"
	"github.com/sniperHW/flyfish/util/str"
	"github.com/sniperHW/kendynet/timer"
	"github.com/sniperHW/kendynet/util"
	"go.etcd.io/etcd/etcdserver/api/snap"
	stats "go.etcd.io/etcd/etcdserver/api/v2stats"
	"go.etcd.io/etcd/pkg/fileutil"
	"go.etcd.io/etcd/pkg/types"
	"go.etcd.io/etcd/raft"
	"go.etcd.io/etcd/raft/raftpb"
	"go.etcd.io/etcd/wal"
	"go.etcd.io/etcd/wal/walpb"
	"go.uber.org/zap"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// A key-value stream backed by raft
type raftNode struct {
	//proposeC    <-chan *proposal         //<-chan []byte            // proposed messages (k,v)
	confChangeC <-chan raftpb.ConfChange // proposed cluster config changes
	commitC     chan<- interface{}       //chan<- *[]byte           // entries committed to log (k,v)
	errorC      chan<- error             // errors from raft session

	nodeID int
	region int

	id        int      // client ID for raft session
	peers     []string // raft peer URLs
	join      bool     // node is joining an existing cluster
	waldir    string   // path to WAL directory
	snapdir   string   // path to snapshot directory
	lastIndex uint64   // index of log at start

	confState     raftpb.ConfState
	snapshotIndex uint64
	appliedIndex  uint64

	// raft backing for the commit/error channel
	node        raft.Node
	raftStorage *raft.MemoryStorage
	wal         *wal.WAL

	getSnapshot func() [][]*kvsnap

	snapshotter      *snap.Snapshotter
	snapshotterReady chan *snap.Snapshotter // signals when snapshotter is ready

	snapCount uint64
	transport *rafthttp.Transport
	stopc     chan struct{} // signals proposal channel closed

	//muSnapshotting sync.Mutex
	snapshotting   bool             //当前是否正在做快照
	entries        [][]raftpb.Entry //快照期间无法添加到storage中的[]raftpb.Entry
	snapshottingOK chan struct{}

	muPendingPropose sync.Mutex
	pendingPropose   *list.List
	proposeIndex     int64

	muLeader sync.Mutex
	leader   int

	proposePipeline *util.BlockQueue
	readPipeline    *util.BlockQueue
	mutilRaft       *mutilRaft

	muPendingRead sync.Mutex
	pendingRead   *list.List

	readIndex int64
	lease     *lease

	term uint64

	gotLeaseCb func()

	kvstore *kvstore

	//cbBecomeLeader   func()
	//cbLoseLeaderShip func()
}

var defaultSnapshotCount uint64 = 10000

// newRaftNode initiates a raft instance and returns a committed log entry
// channel and error channel. Proposals for log updates are sent over the
// provided the proposal channel. All log entries are replayed over the
// commit channel, followed by a nil message (to indicate the channel is
// current), then new log entries. To shutdown, close proposeC and read errorC.
func newRaftNode(mutilRaft *mutilRaft, id int, peers []string, join bool, proposeC *util.BlockQueue,
	confChangeC <-chan raftpb.ConfChange, readC *util.BlockQueue, getSnapshot func() [][]*kvsnap) (*raftNode, <-chan interface{}, <-chan error, <-chan *snap.Snapshotter) {

	/*
	 *  如果commitC设置成无缓冲，则raftNode会等待上层提取commitedEntry之后才继续后续处理。
	 *  flyfish的apply只涉及内存操作，可以给commitC设置缓冲区,使得raftNode快速从投递中返回继续后续工作。
	 */

	commitC := make(chan interface{}, 100)

	errorC := make(chan error)

	nodeID := id >> 16
	region := id & 0xFFFF

	rc := &raftNode{
		confChangeC:      confChangeC,
		commitC:          commitC,
		errorC:           errorC,
		id:               id,
		peers:            peers,
		join:             join,
		waldir:           fmt.Sprintf("kv-%d-%d", nodeID, region),
		snapdir:          fmt.Sprintf("kv-%d-%d-snap", nodeID, region),
		snapCount:        defaultSnapshotCount,
		stopc:            make(chan struct{}),
		snapshotterReady: make(chan *snap.Snapshotter, 1),
		snapshottingOK:   make(chan struct{}),
		pendingPropose:   list.New(),
		mutilRaft:        mutilRaft,
		nodeID:           nodeID,
		region:           region,
		getSnapshot:      getSnapshot,

		pendingRead: list.New(),

		proposePipeline: proposeC,
		readPipeline:    readC,
		lease:           &lease{stop: nil},

		// rest of structure populated after WAL replay
	}
	rc.startProposePipeline()
	rc.startReadPipeline()
	go rc.startRaft()
	return rc, commitC, errorC, rc.snapshotterReady
}

func (rc *raftNode) isLeader() bool {
	rc.muLeader.Lock()
	defer rc.muLeader.Unlock()
	//Infoln("isLeader", rc.leader == rc.id, rc.leader, rc.id)
	return rc.leader == rc.id
}

func (rc *raftNode) getTerm() uint64 {
	rc.muLeader.Lock()
	defer rc.muLeader.Unlock()
	return rc.term
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

func (rc *raftNode) removeOldWal(index uint64) {
	names := readWALNames(rc.waldir)
	if names == nil {
		return
	}

	nameIndex, ok := searchIndex(names, index)
	if ok {
		for _, v := range names[:nameIndex] {
			os.Remove(rc.waldir + "/" + v)
			Infoln("remove old wal", v)
		}
	}
}

func (rc *raftNode) removeOldSnapAndWal(term uint64, index uint64) {
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
							Infoln("remove old snap", path)
							rc.removeOldWal(_index)
						}
					}
					return nil
				}

				return nil
			})
	}()
}

func (rc *raftNode) saveSnap(snap raftpb.Snapshot) error {
	// must save the snapshot index to the WAL before saving the
	// snapshot to maintain the invariant that we only Open the
	// wal at previously-saved snapshot indexes.
	walSnap := walpb.Snapshot{
		Index: snap.Metadata.Index,
		Term:  snap.Metadata.Term,
	}
	if err := rc.wal.SaveSnapshot(walSnap); err != nil {
		return err
	}
	if err := rc.snapshotter.SaveSnap(snap); err != nil {
		return err
	}
	err := rc.wal.ReleaseLockTo(snap.Metadata.Index)

	if nil == err {
		rc.removeOldSnapAndWal(snap.Metadata.Term, snap.Metadata.Index)
	}

	return err
}

func (rc *raftNode) entriesToApply(ents []raftpb.Entry) (nents []raftpb.Entry) {
	if len(ents) == 0 {
		return ents
	}
	firstIdx := ents[0].Index
	if firstIdx > rc.appliedIndex+1 {
		Fatalf("first index of committed entry[%d] should <= progress.appliedIndex[%d]+1", firstIdx, rc.appliedIndex)
	}
	if rc.appliedIndex-firstIdx+1 < uint64(len(ents)) {
		nents = ents[rc.appliedIndex-firstIdx+1:]
	}
	return nents
}

// publishEntries writes committed log entries to commit channel and returns
// whether all entries could be published.
func (rc *raftNode) publishEntries(ents []raftpb.Entry) bool {
	for i := range ents {
		switch ents[i].Type {
		case raftpb.EntryNormal:
			if len(ents[i].Data) == 0 {
				// ignore empty messages
				break
			}

			index := int64(binary.BigEndian.Uint64(ents[i].Data[0:8]))
			committedEntry := &commitedBatchProposal{
				data: ents[i].Data[8:],
			}
			rc.muPendingPropose.Lock()
			front := rc.pendingPropose.Front()
			if nil != front {
				if front.Value.(*batchProposal).index == index {
					committedEntry.tasks = front.Value.(*batchProposal).tasks
					str.Put(front.Value.(*batchProposal).proposalStr)
					rc.pendingPropose.Remove(front)
				}
			}
			rc.muPendingPropose.Unlock()

			select {
			case rc.commitC <- committedEntry:
			case <-rc.stopc:
				return false
			}

		case raftpb.EntryConfChange:
			var cc raftpb.ConfChange
			cc.Unmarshal(ents[i].Data)
			rc.confState = *rc.node.ApplyConfChange(cc)
			Infoln("raftpb.EntryConfChange", cc.Type, cc)
			switch cc.Type {
			case raftpb.ConfChangeAddNode:
				if len(cc.Context) > 0 {
					rc.transport.AddPeer(types.ID(cc.NodeID), []string{string(cc.Context)})
				}
			case raftpb.ConfChangeRemoveNode:
				if cc.NodeID == uint64(rc.id) {
					Infoln("I've been removed from the cluster! Shutting down.")
					return false
				}
				rc.transport.RemovePeer(types.ID(cc.NodeID))
			}
		}

		// after commit, update appliedIndex
		rc.appliedIndex = ents[i].Index

		// special nil commit to signal replay has finished
		if ents[i].Index == rc.lastIndex {
			select {
			case rc.commitC <- replayOK:
				Infoln("send replayOK")
			case <-rc.stopc:
				return false
			}
		}
	}
	return true
}

func (rc *raftNode) loadSnapshot() *raftpb.Snapshot {
	snapshot, err := rc.snapshotter.Load()
	if err != nil && err != snap.ErrNoSnapshot {
		Fatalf("raftexample: error loading snapshot (%v)", err)
	}
	return snapshot
}

// openWAL returns a WAL ready for reading.
func (rc *raftNode) openWAL(snapshot *raftpb.Snapshot) *wal.WAL {
	if !wal.Exist(rc.waldir) {
		if err := os.Mkdir(rc.waldir, 0750); err != nil {
			Fatalf("raftexample: cannot create dir for wal (%v)", err)
		}

		w, err := wal.Create(zap.NewExample(), rc.waldir, nil)
		if err != nil {
			Fatalf("raftexample: create wal error (%v)", err)
		}
		w.Close()
	}

	walsnap := walpb.Snapshot{}
	if snapshot != nil {
		walsnap.Index, walsnap.Term = snapshot.Metadata.Index, snapshot.Metadata.Term
	}

	Infof("loading WAL at term %d and index %d", walsnap.Term, walsnap.Index)
	w, err := wal.Open(zap.NewExample(), rc.waldir, walsnap)
	if err != nil {
		Fatalf("raftexample: error loading wal (%v)", err)
	}

	return w
}

// replayWAL replays WAL entries into the raft instance.
func (rc *raftNode) replayWAL() *wal.WAL {
	Infof("replaying WAL of member %d", rc.id)
	snapshot := rc.loadSnapshot()
	w := rc.openWAL(snapshot)
	_, st, ents, err := w.ReadAll()
	if err != nil {
		Fatalf("raftexample: failed to read WAL (%v)", err)
	} else {
		Infof("ents:%d", len(ents))
	}
	rc.raftStorage = raft.NewMemoryStorage()
	if snapshot != nil {
		rc.raftStorage.ApplySnapshot(*snapshot)
	}
	rc.raftStorage.SetHardState(st)

	if snapshot != nil {
		Infoln("send replaySnapshot")
		rc.commitC <- replaySnapshot
	}

	// append to storage so raft starts at the right place in log
	rc.raftStorage.Append(ents)
	// send nil once lastIndex is published so client knows commit channel is current
	if len(ents) > 0 {
		rc.lastIndex = ents[len(ents)-1].Index
	} else {
		//Infoln("here3")
		rc.commitC <- replayOK //nil
	}
	return w
}

func (rc *raftNode) writeError(err error) {
	//rc.stopHTTP()
	close(rc.commitC)
	rc.errorC <- err
	close(rc.errorC)
	rc.node.Stop()
}

func (rc *raftNode) startRaft() {
	if !fileutil.Exist(rc.snapdir) {
		if err := os.Mkdir(rc.snapdir, 0750); err != nil {
			Fatalf("raftexample: cannot create dir for snapshot (%v)", err)
		}
	}
	rc.snapshotter = snap.New(zap.NewExample(), rc.snapdir)
	rc.snapshotterReady <- rc.snapshotter

	oldwal := wal.Exist(rc.waldir)
	rc.wal = rc.replayWAL()

	rpeers := make([]raft.Peer, len(rc.peers))
	for i := range rpeers {
		id := (i+1)<<16 + rc.region
		rpeers[i] = raft.Peer{ID: uint64(id)}
	}
	c := &raft.Config{
		ID:                        uint64(rc.id),
		ElectionTick:              10,
		HeartbeatTick:             1,
		Storage:                   rc.raftStorage,
		MaxSizePerMsg:             1024 * 1024,
		MaxInflightMsgs:           256,
		MaxUncommittedEntriesSize: 1 << 30,
	}

	if oldwal {
		rc.node = raft.RestartNode(c)
	} else {
		startPeers := rpeers
		if rc.join {
			startPeers = nil
		}
		rc.node = raft.StartNode(c, startPeers)
	}

	rc.transport = &rafthttp.Transport{
		Logger:      zap.NewExample(),
		ID:          types.ID(rc.id),
		ClusterID:   0x10000,
		Raft:        rc,
		ServerStats: stats.NewServerStats("", ""),
		LeaderStats: stats.NewLeaderStats(strconv.Itoa(rc.id)),
		ErrorC:      make(chan error),
	}

	rc.mutilRaft.addTransport(types.ID(rc.id), rc.transport)
	rc.transport.Start()
	for i := range rc.peers {
		id := (i+1)<<16 + rc.region
		if id != rc.id {
			Infoln("AddPeer", types.ID(id).String())
			rc.transport.AddPeer(types.ID(id), []string{rc.peers[i]})
		}
	}

	//go rc.serveRaft()

	timer.Repeat(time.Second, nil, rc.processTimeoutReadReq)

	go rc.serveChannels()
}

// stop closes http, closes all channels, and stops raft.
func (rc *raftNode) stop() {
	rc.transport.Stop()
	rc.mutilRaft.removeTransport(types.ID(rc.id))
	close(rc.commitC)
	close(rc.errorC)
	rc.node.Stop()
}

func (rc *raftNode) publishSnapshot(snapshotToSave raftpb.Snapshot) {
	if raft.IsEmptySnap(snapshotToSave) {
		return
	}

	Infof("publishing snapshot at index %d", rc.snapshotIndex)
	defer Infof("finished publishing snapshot at index %d", rc.snapshotIndex)

	if snapshotToSave.Metadata.Index <= rc.appliedIndex {
		Fatalf("snapshot index [%d] should > progress.appliedIndex [%d]", snapshotToSave.Metadata.Index, rc.appliedIndex)
	}
	rc.commitC <- replaySnapshot // trigger kvstore to load snapshot

	rc.confState = snapshotToSave.Metadata.ConfState
	rc.snapshotIndex = snapshotToSave.Metadata.Index
	rc.appliedIndex = snapshotToSave.Metadata.Index
}

var snapshotCatchUpEntriesN uint64 = 10000

func (rc *raftNode) onTriggerSnapshotOK() {

	compactIndex := uint64(1)
	if rc.appliedIndex > snapshotCatchUpEntriesN {
		compactIndex = rc.appliedIndex - snapshotCatchUpEntriesN
	}
	if err := rc.raftStorage.Compact(compactIndex); err != nil {
		panic(err)
	}
	rc.snapshotIndex = rc.appliedIndex

	rc.snapshotting = false

}

func (rc *raftNode) maybeTriggerSnapshot() {

	if rc.snapshotting {
		return
	}

	if rc.appliedIndex-rc.snapshotIndex <= rc.snapCount {
		return
	}

	clone := rc.getSnapshot()

	if nil == clone {
		return
	}

	rc.snapshotting = true

	Infof("start snapshot [applied index: %d | last snapshot index: %d]", rc.appliedIndex, rc.snapshotIndex)

	appliedIndex := rc.appliedIndex
	confState := rc.confState

	go func() {

		ss := str.Get()
		ss.AppendInt64(0)

		for _, v := range clone {
			for _, vv := range v {
				vv.append2Str(ss)
				//ss.appendProposal(proposal_snapshot, vv.uniKey, vv.values, vv.version)
			}
		}

		beg := time.Now()

		snap, err := rc.raftStorage.CreateSnapshot(appliedIndex, &confState, ss.Bytes())
		if err != nil {
			panic(err)
		}

		if err := rc.saveSnap(snap); err != nil {
			panic(err)
		}

		str.Put(ss)

		Infoln("save snapshot time", time.Now().Sub(beg))

		rc.snapshottingOK <- struct{}{}

	}()

}

func (rc *raftNode) onLoseLeadership() {

	rc.lease.loseLeaderShip()
	//rc.cbLoseLeaderShip()

	rc.muPendingPropose.Lock()
	pendingPropose := rc.pendingPropose
	rc.pendingPropose = list.New()
	rc.muPendingPropose.Unlock()

	for e := pendingPropose.Front(); e != nil; e = e.Next() {
		v := e.Value.(*batchProposal)
		v.onError(errcode.ERR_NOT_LEADER)
	}

	rc.muPendingRead.Lock()
	pendingRead := rc.pendingRead
	rc.pendingRead = list.New()
	rc.muPendingRead.Unlock()

	for e := pendingRead.Front(); e != nil; e = e.Next() {
		c := e.Value.(*readBatchSt)
		c.onError(errcode.ERR_NOT_LEADER)
	}
}

func (rc *raftNode) issueRead(tasks *fixedarray.FixedArray) {

	Debugln("issueRead")

	ctxToSend := make([]byte, 8)
	c := &readBatchSt{
		readIndex: atomic.AddInt64(&rc.readIndex, 1),
		deadline:  time.Now().Add(time.Second * 5),
		tasks:     tasks,
	}

	binary.BigEndian.PutUint64(ctxToSend, uint64(c.readIndex))

	rc.muPendingRead.Lock()
	e := rc.pendingRead.PushBack(c)
	rc.muPendingRead.Unlock()

	cctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

	if err := rc.node.ReadIndex(cctx, ctxToSend); err != nil {
		cancel()
		code := errcode.ERR_RAFT
		if err == raft.ErrStopped {
			code = errcode.ERR_SERVER_STOPED
		} else if err == cctx.Err() {
			code = errcode.ERR_TIMEOUT
		}

		rc.muPendingRead.Lock()
		rc.pendingRead.Remove(e)
		rc.muPendingRead.Unlock()
		c.onError(code)
		return
	}
	cancel()
}

func (rc *raftNode) startReadPipeline() {

	sleepTime := time.Duration(conf.GetConfig().ReadFlushInterval)

	go func() {
		for {
			time.Sleep(time.Millisecond * sleepTime)
			//发送信号，触发batch
			if rc.readPipeline.AddNoWait(nil) != nil {
				return
			}
		}
	}()

	go func() {

		tasks := fixedArrayPool.Get()

		for {
			closed, localList := rc.readPipeline.Get()

			for _, vv := range localList {
				if vv == nil {
					if !tasks.Empty() {
						rc.issueRead(tasks)
						tasks = fixedArrayPool.Get()
					}
				} else {
					if !rc.isLeader() {
						vv.(asynTaskI).onError(errcode.ERR_NOT_LEADER)
					} else {
						tasks.Append(vv)
						if tasks.Full() {
							rc.issueRead(tasks)
							tasks = fixedArrayPool.Get()
						}
					}
				}
			}

			if closed {
				return
			}
		}
	}()
}

func (rc *raftNode) issuePropose(batch *batchProposal) {

	rc.muPendingPropose.Lock()
	e := rc.pendingPropose.PushBack(batch)
	rc.muPendingPropose.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	err := rc.node.Propose(ctx, batch.proposalStr.Bytes())
	cancel()

	if nil != err {
		if err == ctx.Err() {
			batch.onPorposeTimeout()
		} else {

			rc.muPendingPropose.Lock()
			rc.pendingPropose.Remove(e)
			rc.muPendingPropose.Unlock()

			var errCode int32

			switch err {
			case raft.ErrStopped:
				errCode = errcode.ERR_SERVER_STOPED
			case raft.ErrProposalDropped:
				if !rc.isLeader() {
					errCode = errcode.ERR_NOT_LEADER
				} else {
					errCode = errcode.ERR_PROPOSAL_DROPPED
				}
			default:
				errCode = errcode.ERR_RAFT
			}
			batch.onError(errCode)
		}
	}
}

func (rc *raftNode) newBatchProposal() *batchProposal {
	index := atomic.AddInt64(&rc.proposeIndex, 1)

	return &batchProposal{
		index:       index,
		tasks:       fixedArrayPool.Get(),
		proposalStr: str.Get().AppendInt64(index),
	}
}

func (rc *raftNode) startProposePipeline() {

	sleepTime := time.Duration(conf.GetConfig().ProposalFlushInterval)

	go func() {
		for {
			time.Sleep(time.Millisecond * sleepTime)
			//发送信号，触发batch提交
			if rc.proposePipeline.AddNoWait(nil) != nil {
				return
			}
		}
	}()

	go func() {

		batch := rc.newBatchProposal()

		for {
			closed, localList := rc.proposePipeline.Get()
			for _, vv := range localList {
				if nil == vv {
					if !batch.tasks.Empty() {
						rc.issuePropose(batch)
						batch = rc.newBatchProposal()
					}
				} else {
					if !rc.isLeader() {
						vv.(asynTaskI).onError(errcode.ERR_NOT_LEADER)
					} else {
						batch.tasks.Append(vv)
						vv.(asynTaskI).append2Str(batch.proposalStr)

						switch vv.(type) {
						case asynTaskLease:
							rc.issuePropose(batch)
							batch = rc.newBatchProposal()
						default:
							if batch.tasks.Full() {
								rc.issuePropose(batch)
								batch = rc.newBatchProposal()
							}
						}
					}
				}
			}

			if closed {
				return
			}
		}

	}()
}

func (rc *raftNode) processTimeoutReadReq(_ *timer.Timer) {
	for {
		rc.muPendingRead.Lock()
		e := rc.pendingRead.Front()
		if e == nil {
			rc.muPendingRead.Unlock()
			return
		}

		now := time.Now()
		if now.After(e.Value.(*readBatchSt).deadline) {
			c := e.Value.(*readBatchSt)
			rc.pendingRead.Remove(e)
			rc.muPendingRead.Unlock()
			Debugln("read timeout")
			c.onError(errcode.ERR_TIMEOUT)
		} else {
			rc.muPendingRead.Unlock()
			return
		}
	}
}

func (rc *raftNode) processReadStates(readStates []raft.ReadState) {
	for _, rs := range readStates {
		rc.muPendingRead.Lock()
		e := rc.pendingRead.Front()
		index := int64(binary.BigEndian.Uint64(rs.RequestCtx))
		if nil == e || e.Value.(*readBatchSt).readIndex != index {
			//上下文已经因为超时被移除
			rc.muPendingRead.Unlock()
		} else {
			c := e.Value.(*readBatchSt)
			rc.pendingRead.Remove(e)
			rc.muPendingRead.Unlock()
			Debugln("readStates")
			select {
			case rc.commitC <- c:
			case <-rc.stopc:
			}
		}
	}
}

func (rc *raftNode) serveChannels() {
	snap, err := rc.raftStorage.Snapshot()
	if err != nil {
		panic(err)
	}
	rc.confState = snap.Metadata.ConfState
	rc.snapshotIndex = snap.Metadata.Index
	rc.appliedIndex = snap.Metadata.Index

	defer rc.wal.Close()

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	// send proposals over raft
	go func() {
		confChangeCount := uint64(0)
		for rc.confChangeC != nil {
			select {
			case cc, ok := <-rc.confChangeC:
				if !ok {
					rc.confChangeC = nil
				} else {
					confChangeCount++
					cc.ID = confChangeCount
					rc.node.ProposeConfChange(context.TODO(), cc)
				}
			}
		}
		// client closed channel; shutdown raft if not already
		close(rc.stopc)
	}()

	// event loop on raft state machine updates
	for {
		select {
		case <-ticker.C:
			rc.node.Tick()

		// store raft entries to wal, then publish over commit channel
		case rd := <-rc.node.Ready():
			loseLeadership := false
			if rd.SoftState != nil {
				rc.muLeader.Lock()
				oldLeader := rc.leader
				rc.leader = int(rd.SoftState.Lead)
				rc.term = rd.HardState.Term
				rc.muLeader.Unlock()

				if oldLeader == rc.id && rc.leader != rc.id {
					loseLeadership = true
				}
				Infoln(rd.SoftState.Lead>>16, "is leader")

				if rc.leader == rc.id {
					rc.lease.becomeLeader(rc)
					//rc.cbBecomeLeader()
				}

			}

			rc.wal.Save(rd.HardState, rd.Entries)

			if !raft.IsEmptySnap(rd.Snapshot) {
				rc.saveSnap(rd.Snapshot)
				rc.raftStorage.ApplySnapshot(rd.Snapshot)
				rc.publishSnapshot(rd.Snapshot)
			}

			rc.raftStorage.Append(rd.Entries)

			rc.transport.Send(rd.Messages)

			if ok := rc.publishEntries(rc.entriesToApply(rd.CommittedEntries)); !ok {
				rc.stop()
				return
			}

			if len(rd.ReadStates) != 0 {
				rc.processReadStates(rd.ReadStates)
			}

			rc.maybeTriggerSnapshot()

			if loseLeadership {
				rc.onLoseLeadership()
			}

			rc.node.Advance()

		case err := <-rc.transport.ErrorC:
			rc.writeError(err)
			return

		case <-rc.stopc:
			rc.stop()
			return
		case <-rc.snapshottingOK:
			rc.onTriggerSnapshotOK()
		}
	}
}

func (rc *raftNode) renew() {
	rc.proposePipeline.AddNoWait(&asynTaskLease{
		rn:   rc,
		term: rc.getTerm(),
	})
}

func (rc *raftNode) hasLease() bool {
	return rc.lease.hasLease(rc)
}

func (rc *raftNode) gotLease() {
	rc.gotLeaseCb()
}

func (rc *raftNode) Process(ctx context.Context, m raftpb.Message) error {
	return rc.node.Step(ctx, m)
}
func (rc *raftNode) IsIDRemoved(id uint64) bool                           { return false }
func (rc *raftNode) ReportUnreachable(id uint64)                          {}
func (rc *raftNode) ReportSnapshot(id uint64, status raft.SnapshotStatus) {}
