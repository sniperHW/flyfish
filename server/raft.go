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

package server

import (
	"container/list"
	"context"
	"encoding/binary"
	"fmt"
	"github.com/sniperHW/flyfish/errcode"
	"github.com/sniperHW/flyfish/rafthttp"
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
	"log"
	//"net/http"
	//"net/url"
	"github.com/sniperHW/kendynet/timer"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var replayOK *commitedBatchBinlog = &commitedBatchBinlog{}
var replaySnapshot *commitedBatchBinlog = &commitedBatchBinlog{}

// A key-value stream backed by raft
type raftNode struct {
	proposeC    <-chan *batchBinlog      //<-chan []byte            // proposed messages (k,v)
	confChangeC <-chan raftpb.ConfChange // proposed cluster config changes
	commitC     chan<- interface{}       //chan<- *[]byte           // entries committed to log (k,v)
	errorC      chan<- error             // errors from raft session

	nodeID int
	region int

	id          int      // client ID for raft session
	peers       []string // raft peer URLs
	join        bool     // node is joining an existing cluster
	waldir      string   // path to WAL directory
	snapdir     string   // path to snapshot directory
	getSnapshot func() [][]kvsnap
	lastIndex   uint64 // index of log at start

	confState     raftpb.ConfState
	snapshotIndex uint64
	appliedIndex  uint64

	// raft backing for the commit/error channel
	node        raft.Node
	raftStorage *raft.MemoryStorage
	wal         *wal.WAL

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
	pendingPropose   *list.List //[]*batchBinlog
	proposeIndex     int64

	muLeader sync.Mutex
	leader   int

	pipeline  *util.BlockQueue
	mutilRaft *mutilRaft

	muPendingRead sync.Mutex
	pendingRead   *list.List
	readC         <-chan *readBatchSt
	readIndex     int64
}

var defaultSnapshotCount uint64 = 10000

// newRaftNode initiates a raft instance and returns a committed log entry
// channel and error channel. Proposals for log updates are sent over the
// provided the proposal channel. All log entries are replayed over the
// commit channel, followed by a nil message (to indicate the channel is
// current), then new log entries. To shutdown, close proposeC and read errorC.
func newRaftNode(mutilRaft *mutilRaft, id int, peers []string, join bool, getSnapshot func() [][]kvsnap, proposeC <-chan *batchBinlog,
	confChangeC <-chan raftpb.ConfChange) (*raftNode, <-chan interface{}, <-chan error, <-chan *snap.Snapshotter, chan<- *readBatchSt) {

	commitC := make(chan interface{})
	errorC := make(chan error)
	readC := make(chan *readBatchSt, 100)

	nodeID := id >> 16
	region := id & 0xFFFF

	rc := &raftNode{
		proposeC:    proposeC,
		confChangeC: confChangeC,
		commitC:     commitC,
		errorC:      errorC,
		id:          id,
		peers:       peers,
		join:        join,
		waldir:      fmt.Sprintf("raftexample-%d-%d", nodeID, region),
		snapdir:     fmt.Sprintf("raftexample-%d-%d-snap", nodeID, region),
		getSnapshot: getSnapshot,
		snapCount:   defaultSnapshotCount,
		stopc:       make(chan struct{}),
		//httpstopc:        make(chan struct{}),
		//httpdonec:        make(chan struct{}),
		snapshotterReady: make(chan *snap.Snapshotter, 1),
		snapshottingOK:   make(chan struct{}),
		pendingPropose:   list.New(), //[]*batchBinlog{}
		mutilRaft:        mutilRaft,
		nodeID:           nodeID,
		region:           region,

		pendingRead: list.New(),
		readC:       readC,

		// rest of structure populated after WAL replay
	}
	rc.startProposePipeline()
	go rc.startRaft()
	return rc, commitC, errorC, rc.snapshotterReady, readC
}

func (rc *raftNode) isLeader() bool {
	rc.muLeader.Lock()
	defer rc.muLeader.Unlock()
	return rc.leader == rc.id
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
		log.Fatalf("first index of committed entry[%d] should <= progress.appliedIndex[%d]+1", firstIdx, rc.appliedIndex)
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
			committedEntry := &commitedBatchBinlog{
				data: ents[i].Data[8:],
			}
			rc.muPendingPropose.Lock()
			front := rc.pendingPropose.Front()
			if nil != front {
				if front.Value.(*batchBinlog).index == index {
					committedEntry.ctxs = front.Value.(*batchBinlog).ctxs
					strPut(front.Value.(*batchBinlog).binlogStr)
					rc.pendingPropose.Remove(front)
				} else {
					Infoln("index not match", index, front.Value.(*batchBinlog).index)
				}
			}
			rc.muPendingPropose.Unlock()

			//committedEntry.Index = ents[i].Index

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
					log.Println("I've been removed from the cluster! Shutting down.")
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
		log.Fatalf("raftexample: error loading snapshot (%v)", err)
	}
	return snapshot
}

// openWAL returns a WAL ready for reading.
func (rc *raftNode) openWAL(snapshot *raftpb.Snapshot) *wal.WAL {
	if !wal.Exist(rc.waldir) {
		if err := os.Mkdir(rc.waldir, 0750); err != nil {
			log.Fatalf("raftexample: cannot create dir for wal (%v)", err)
		}

		w, err := wal.Create(zap.NewExample(), rc.waldir, nil)
		if err != nil {
			log.Fatalf("raftexample: create wal error (%v)", err)
		}
		w.Close()
	}

	walsnap := walpb.Snapshot{}
	if snapshot != nil {
		walsnap.Index, walsnap.Term = snapshot.Metadata.Index, snapshot.Metadata.Term
	} else {
		log.Printf("snapshot == nil")
	}
	log.Printf("loading WAL at term %d and index %d", walsnap.Term, walsnap.Index)
	w, err := wal.Open(zap.NewExample(), rc.waldir, walsnap)
	if err != nil {
		log.Fatalf("raftexample: error loading wal (%v)", err)
	}

	return w
}

// replayWAL replays WAL entries into the raft instance.
func (rc *raftNode) replayWAL() *wal.WAL {
	log.Printf("replaying WAL of member %d", rc.id)
	snapshot := rc.loadSnapshot()
	w := rc.openWAL(snapshot)
	_, st, ents, err := w.ReadAll()
	if err != nil {
		log.Fatalf("raftexample: failed to read WAL (%v)", err)
	} else {
		log.Printf("ents:%d", len(ents))
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
			log.Fatalf("raftexample: cannot create dir for snapshot (%v)", err)
		}
	}
	rc.snapshotter = snap.New(zap.NewExample(), rc.snapdir)
	rc.snapshotterReady <- rc.snapshotter

	oldwal := wal.Exist(rc.waldir)
	rc.wal = rc.replayWAL()

	rpeers := make([]raft.Peer, len(rc.peers))
	for i := range rpeers {
		rpeers[i] = raft.Peer{ID: uint64(rc.id)}
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

	log.Printf("publishing snapshot at index %d", rc.snapshotIndex)
	defer log.Printf("finished publishing snapshot at index %d", rc.snapshotIndex)

	if snapshotToSave.Metadata.Index <= rc.appliedIndex {
		log.Fatalf("snapshot index [%d] should > progress.appliedIndex [%d]", snapshotToSave.Metadata.Index, rc.appliedIndex)
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

	log.Printf("start snapshot [applied index: %d | last snapshot index: %d]", rc.appliedIndex, rc.snapshotIndex)

	appliedIndex := rc.appliedIndex
	confState := rc.confState

	go func() {

		ss := strGet()
		ss.appendInt64(0)

		for _, v := range clone {
			for _, vv := range v {
				//if "" == vv.uniKey || vv.version == 0 {
				//	panic("error")
				//}
				ss.appendBinLog(binlog_snapshot, vv.uniKey, vv.values, vv.version)
			}
		}

		beg := time.Now()

		snap, err := rc.raftStorage.CreateSnapshot(appliedIndex, &confState, ss.bytes())
		if err != nil {
			panic(err)
		}

		if err := rc.saveSnap(snap); err != nil {
			panic(err)
		}

		strPut(ss)

		Infoln("save snapshot time", time.Now().Sub(beg))

		rc.snapshottingOK <- struct{}{}

	}()

}

func (rc *raftNode) onLoseLeadership() {
	rc.muPendingPropose.Lock()
	pendingPropose := rc.pendingPropose
	rc.pendingPropose = list.New()
	rc.muPendingPropose.Unlock()

	for e := pendingPropose.Front(); e != nil; e = e.Next() {
		v := e.Value.(*batchBinlog)
		strPut(v.binlogStr)
		if nil != v.ctxs {
			for i := 0; i < v.ctxs.count; i++ {
				ctx := v.ctxs.ctxs[i]
				if ctx.getCmdType() == cmdKick {
					ctx.getCacheKey().clearKicking()
				} else {
					ctx.reply(errcode.ERR_NOT_LEADER, nil, 0)
				}
				ctx.getCacheKey().processQueueCmd()
			}
			ctxArrayPut(v.ctxs)
		}
	}

	rc.muPendingRead.Lock()
	pendingRead := rc.pendingRead
	rc.pendingRead = list.New()
	rc.muPendingRead.Unlock()

	for e := pendingRead.Front(); e != nil; e = e.Next() {
		c := e.Value.(*readBatchSt)
		for i := 0; i < c.ctxs.count; i++ {
			v := c.ctxs.ctxs[i]
			v.reply(int32(errcode.ERR_NOT_LEADER), nil, -1)
			v.getCacheKey().processQueueCmd()
		}
		ctxArrayPut(c.ctxs)

	}
}

func (rc *raftNode) startProposePipeline() {
	rc.pipeline = util.NewBlockQueue()

	go func() {

		for {
			closed, localList := rc.pipeline.Get()
			for _, vv := range localList {

				prop := vv.(*batchBinlog)

				if !rc.isLeader() {
					strPut(prop.binlogStr)
					for i := 0; i < prop.ctxs.count; i++ {
						v := prop.ctxs.ctxs[i]
						if v.getCmdType() != cmdKick {
							v.reply(errcode.ERR_NOT_LEADER, nil, 0)
						} else {
							v.getCacheKey().clearKicking()
						}
						v.getCacheKey().processQueueCmd()
					}
					ctxArrayPut(prop.ctxs)
				} else {
					// blocks until accepted by raft state machine
					prop.index = atomic.AddInt64(&rc.proposeIndex, 1)
					binary.BigEndian.PutUint64(prop.binlogStr.data[:8], uint64(prop.index))
					rc.muPendingPropose.Lock()
					e := rc.pendingPropose.PushBack(prop)
					rc.muPendingPropose.Unlock()
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					err := rc.node.Propose(ctx, prop.binlogStr.bytes())
					cancel()

					if nil != err {
						if err == ctx.Err() {

							/*   timeout
							 *   只是对客户端超时,复制处理还在继续
							 *   所以只向客户端返回response
							 */
							for i := 0; i < prop.ctxs.count; i++ {
								v := prop.ctxs.ctxs[i]
								if v.getCmdType() != cmdKick {
									v.reply(errcode.ERR_TIMEOUT, nil, 0)
								} else {
									v.getCacheKey().clearKicking()
								}
								v.getCacheKey().processQueueCmd()
							}
						} else {

							rc.muPendingPropose.Lock()
							rc.pendingPropose.Remove(e)
							rc.muPendingPropose.Unlock()

							errCode := int32(0)

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

							strPut(prop.binlogStr)

							for i := 0; i < prop.ctxs.count; i++ {
								v := prop.ctxs.ctxs[i]
								if v.getCmdType() != cmdKick {
									v.reply(errCode, nil, 0)
								} else {
									v.getCacheKey().clearKicking()
								}
								v.getCacheKey().processQueueCmd()
							}
							ctxArrayPut(prop.ctxs)
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
			for i := 0; i < c.ctxs.count; i++ {
				v := c.ctxs.ctxs[i]
				v.reply(errcode.ERR_TIMEOUT, nil, 0)
				v.getCacheKey().processQueueCmd()
			}
			ctxArrayPut(c.ctxs)
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

			/*for i := 0; i < c.ctxs.count; i++ {
				v := c.ctxs.ctxs[i]
				ckey := v.getCacheKey()
				if !rc.isLeader() {
					v.reply(errcode.ERR_NOT_LEADER, nil, -1)
				} else {
					ckey.mtx.Lock()
					if ckey.status == cache_missing {
						v.reply(errcode.ERR_NOTFOUND, nil, -1)
					} else {
						v.reply(errcode.ERR_OK, ckey.values, ckey.version)
					}
					ckey.mtx.Unlock()
				}
				ckey.processQueueCmd()
			}
			ctxArrayPut(c.ctxs)*/

			select {
			case rc.commitC <- c:
			case <-rc.stopc:
			}
		}
	}
}

func (rc *raftNode) issueReadFailed(c *readBatchSt, err int) {
	for i := 0; i < c.ctxs.count; i++ {
		v := c.ctxs.ctxs[i]
		v.reply(int32(err), nil, -1)
		v.getCacheKey().processQueueCmd()
	}
	ctxArrayPut(c.ctxs)
}

func (rc *raftNode) issueRead(c *readBatchSt) {

	if !rc.isLeader() {
		rc.issueReadFailed(c, errcode.ERR_NOT_LEADER)
		return
	}

	ctxToSend := make([]byte, 8)
	c.readIndex = atomic.AddInt64(&rc.readIndex, 1)
	c.deadline = time.Now().Add(time.Second * 5)
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
		rc.issueReadFailed(c, code)
		return
	}
	cancel()
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

		for rc.proposeC != nil && rc.confChangeC != nil {
			select {
			case prop, ok := <-rc.proposeC:
				if !ok {
					rc.proposeC = nil
				} else {
					rc.pipeline.AddNoWait(prop)
				}

			case cc, ok := <-rc.confChangeC:
				if !ok {
					rc.confChangeC = nil
				} else {
					confChangeCount++
					cc.ID = confChangeCount
					rc.node.ProposeConfChange(context.TODO(), cc)
				}
			case readReq, ok := <-rc.readC:
				if ok {
					rc.issueRead(readReq)
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
				rc.muLeader.Unlock()

				if oldLeader == rc.id && rc.leader != rc.id {
					loseLeadership = true
				}
				Infoln(rd.SoftState.Lead, "is leader")
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

func (rc *raftNode) Process(ctx context.Context, m raftpb.Message) error {
	return rc.node.Step(ctx, m)
}
func (rc *raftNode) IsIDRemoved(id uint64) bool                           { return false }
func (rc *raftNode) ReportUnreachable(id uint64)                          {}
func (rc *raftNode) ReportSnapshot(id uint64, status raft.SnapshotStatus) {}
