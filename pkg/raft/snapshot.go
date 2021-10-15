package raft

import (
	//"github.com/sniperHW/flyfish/pkg/buffer"

	"fmt"
	"github.com/dustin/go-humanize"
	"github.com/sniperHW/flyfish/pkg/buffer"
	"go.etcd.io/etcd/etcdserver/api/snap"
	"go.etcd.io/etcd/pkg/types"
	"go.etcd.io/etcd/raft"
	"go.etcd.io/etcd/raft/raftpb"
	"go.etcd.io/etcd/wal/walpb"
	"go.uber.org/zap"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync/atomic"
	"time"
)

type snapshotNotifyst struct {
	applyIdx uint64
	snapshot []byte
}

type SnapshotNotify struct {
	notify snapshotNotifyst
	ch     chan interface{}
	rc     *RaftNode
}

func (this *SnapshotNotify) Notify(snapshot []byte) {
	GetSugar().Infof("snapshot notify")
	this.notify.snapshot = snapshot
	select {
	case this.ch <- this.notify:
		GetSugar().Infof("snapshot notify ok")
	case <-this.rc.stopc:
	}
}

func (rc *RaftNode) maybeTriggerSnapshot(index uint64) bool {

	if atomic.LoadInt64(&rc.snapshotMerging) != 0 {
		//正在执行日志文件合并，不应该触发产生新的日志文件
		return false
	}

	if rc.snapshotting {
		return false
	}

	if index-rc.snapshotIndex <= rc.snapCount {
		return false
	}

	GetSugar().Infof("maybeTriggerSnapshot %d %d", index, rc.snapshotIndex)

	rc.snapshotting = true

	return true
}

func (rc *RaftNode) getSnapFiles() []string {
	snapfiles := []string{}

	filepath.Walk(rc.snapdir,
		func(path string, f os.FileInfo, err error) error {

			if f == nil {
				return err
			}

			if !f.IsDir() && strings.HasSuffix(path, ".snap") {
				filename := strings.TrimLeft(path, rc.snapdir+"/")
				snapfiles = append(snapfiles, filename)
				return nil
			}

			return nil
		})

	//对snap文件按文件名排序

	sort.Slice(snapfiles, func(i, j int) bool { return snapfiles[i] < snapfiles[j] })
	return snapfiles
}

func (rc *RaftNode) onTriggerSnapshotOK(snap raftpb.Snapshot) {

	GetSugar().Infof("onTriggerSnapshotOK")

	compactIndex := uint64(1)
	if snap.Metadata.Index > SnapshotCatchUpEntriesN {
		compactIndex = snap.Metadata.Index - SnapshotCatchUpEntriesN
	}

	rc.snapshotIndex = snap.Metadata.Index

	rc.snapshotting = false

	// When sending a snapshot, etcd will pause compaction.
	// After receives a snapshot, the slow follower needs to get all the entries right after
	// the snapshot sent to catch up. If we do not pause compaction, the log entries right after
	// the snapshot sent might already be compacted. It happens when the snapshot takes long time
	// to send and save. Pausing compaction avoids triggering a snapshot sending cycle.
	if atomic.LoadInt64(&rc.inflightSnapshots) != 0 {
		return
	}

	if err := rc.raftStorage.Compact(compactIndex); err != nil {
		panic(err)
	}

	if nil != rc.snapMerge {
		if !atomic.CompareAndSwapInt64(&rc.snapshotMerging, 0, 1) {
			return
		}

		snapfiles := rc.getSnapFiles()

		go func() {
			if err := rc.mergeSnapshot(snapfiles); nil != err {
				GetSugar().Errorf("mergeSnapshot error:%v", err)
			}
			atomic.StoreInt64(&rc.snapshotMerging, 0)
		}()
	}
}

func (rc *RaftNode) triggerSnapshot(st snapshotNotifyst) {
	GetSugar().Infof("triggerSnapshot")

	var err error
	var snap raftpb.Snapshot

	snap, err = rc.raftStorage.CreateSnapshot(st.applyIdx, &rc.confState, st.snapshot)
	if err != nil {
		panic(err)
	}

	go func() {

		if err = rc.saveSnap(snap); err != nil {
			panic(err)
		}

		if nil == rc.snapMerge {
			//删除旧快照
			rc.removeOldSnapAndWal(snap.Metadata.Term, snap.Metadata.Index-SnapshotCatchUpEntriesN)
		}

		select {
		case rc.snapshotCh <- snap:
		case <-rc.stopc:
		}

	}()
}

func (rc *RaftNode) saveSnap(snap raftpb.Snapshot) error {
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

	return rc.wal.ReleaseLockTo(snap.Metadata.Index)
}

func (rc *RaftNode) loadSnapshot() *raftpb.Snapshot {

	if nil != rc.snapMerge {
		if err := rc.mergeSnapshot(rc.getSnapFiles()); nil != err {
			GetSugar().Fatalf("raftexample: error merge snapshot (%v)", err)
		}
	}

	snapshot, err := rc.snapshotter.Load()
	if err != nil && err != snap.ErrNoSnapshot {
		GetSugar().Fatalf("raftexample: error loading snapshot (%v)", err)
	}

	return snapshot
}

func (rc *RaftNode) publishSnapshot(snapshotToSave raftpb.Snapshot) {
	if raft.IsEmptySnap(snapshotToSave) {
		return
	}

	GetSugar().Infof("publishing snapshot at index %d", rc.snapshotIndex)
	defer GetSugar().Infof("finished publishing snapshot at index %d", rc.snapshotIndex)

	if snapshotToSave.Metadata.Index <= rc.appliedIndex {
		GetSugar().Fatalf("snapshot index [%d] should > progress.appliedIndex [%d]", snapshotToSave.Metadata.Index, rc.appliedIndex)
	}

	// trigger kvstore to load snapshot
	rc.commitC.AppendHighestPriotiryItem(snapshotToSave)

	rc.confState = snapshotToSave.Metadata.ConfState
	rc.snapshotIndex = snapshotToSave.Metadata.Index
	rc.appliedIndex = snapshotToSave.Metadata.Index
}

func newSnapshotReaderCloser(data []byte) io.ReadCloser {
	pr, pw := io.Pipe()
	go func() {
		offset := 0
		for {
			n, err := pw.Write(data[offset:])
			if nil != err {
				pw.CloseWithError(err)
				GetLogger().Warn(
					"failed to send database snapshot to writer",
					zap.String("size", humanize.Bytes(uint64(offset))),
					zap.Error(err),
				)
				return
			} else {
				offset += n
				if offset >= len(data) {
					pw.CloseWithError(nil)
					return
				}
			}
		}
	}()
	return pr
}

func (rc *RaftNode) sendSnapshot(m raftpb.Message, snapfiles []string) {
	b := buffer.New()

	if nil == rc.snapMerge {
		//在不开启快照合并的情况下，每个快照都是全量快照，直接发送最后一个快照即可
		v := snapfiles[len(snapfiles)-1]
		var _term uint64
		var _index uint64

		n, err := fmt.Sscanf(v, "%016x-%016x.snap", &_term, &_index)

		if n != 2 || nil != err {
			panic("invaild snap file")
		}

		if s, err := snap.Read(GetLogger(), rc.snapdir+"/"+v); nil != err {
			GetSugar().Fatalf("read snap %s error:%v", v, err)
		} else {
			b.AppendBytes(s.Data)
		}
	} else {
		for _, v := range snapfiles {

			var _term uint64
			var _index uint64

			n, err := fmt.Sscanf(v, "%016x-%016x.snap", &_term, &_index)

			if n != 2 || nil != err {
				continue
			}

			if s, err := snap.Read(GetLogger(), rc.snapdir+"/"+v); nil != err {
				GetSugar().Fatalf("read snap %s error:%v", v, err)
			} else {
				b.AppendBytes(s.Data)
			}
		}
	}

	m.Snapshot.Data = nil

	pr := newSnapshotReaderCloser(b.Bytes())

	snapMsg := *snap.NewMessage(m, pr, int64(b.Len()))

	now := time.Now()
	rc.transport.SendSnapshot(snapMsg)

	fields := []zap.Field{
		zap.Int("from", rc.ID()),
		zap.String("to", types.ID(snapMsg.To).String()),
		zap.Int64("bytes", snapMsg.TotalSize),
		zap.String("size", humanize.Bytes(uint64(snapMsg.TotalSize))),
	}

	go func() {
		select {
		case ok := <-snapMsg.CloseNotify():
			// delay releasing inflight snapshot for another 30 seconds to
			// block log compaction.
			// If the follower still fails to catch up, it is probably just too slow
			// to catch up. We cannot avoid the snapshot cycle anyway.
			if ok {
				select {
				case <-time.After(ReleaseDelayAfterSnapshot):
				case <-rc.stopping:
				}
			}

			atomic.AddInt64(&rc.inflightSnapshots, -1)

			GetLogger().Info("sent merged snapshot", append(fields, zap.Duration("took", time.Since(now)))...)

		case <-rc.stopping:
			GetLogger().Warn("canceled sending merged snapshot; server stopping", fields...)
			return
		}
	}()
}

func (rc *RaftNode) mergeSnapshot(snapfiles []string) error {
	snapshots := []*raftpb.Snapshot{}
	datas := [][]byte{}

	for _, v := range snapfiles {
		if s, err := snap.Read(GetLogger(), rc.snapdir+"/"+v); nil != err {
			return err
		} else {
			snapshots = append(snapshots, s)
			datas = append(datas, s.Data)
		}
	}

	if len(snapshots) <= 1 {
		return nil
	}

	mergeSnap := raftpb.Snapshot{
		Metadata: snapshots[len(snapshots)-1].Metadata,
	}

	if mergeData, err := rc.snapMerge(datas...); nil != err {
		return err
	} else {
		mergeSnap.Data = mergeData
		err = rc.snapshotter.SaveSnap(mergeSnap)
		if nil == err {
			rc.removeOldSnapAndWal(mergeSnap.Metadata.Term, mergeSnap.Metadata.Index-SnapshotCatchUpEntriesN)
		}
		return err
	}
}
