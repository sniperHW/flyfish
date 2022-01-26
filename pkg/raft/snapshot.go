package raft

import (
	"github.com/dustin/go-humanize"
	"github.com/sniperHW/flyfish/pkg/buffer"
	"github.com/sniperHW/flyfish/pkg/etcd/etcdserver/api/snap"
	"github.com/sniperHW/flyfish/pkg/etcd/pkg/types"
	"github.com/sniperHW/flyfish/pkg/etcd/raft"
	"github.com/sniperHW/flyfish/pkg/etcd/raft/raftpb"
	"github.com/sniperHW/flyfish/pkg/etcd/wal"
	"github.com/sniperHW/flyfish/pkg/etcd/wal/walpb"
	"go.uber.org/zap"
	"io"
	//"sort"
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
	rc     *RaftInstance
}

func (this *SnapshotNotify) Notify(snapshot []byte) {
	GetSugar().Debugf("snapshot notify")
	this.notify.snapshot = snapshot
	select {
	case this.ch <- this.notify:
		GetSugar().Debugf("snapshot notify ok")
	case <-this.rc.stopc:
	}
}

func (rc *RaftInstance) maybeTriggerSnapshot(index uint64) bool {
	if rc.snapshotting {
		return false
	}

	if index-rc.snapshotIndex <= rc.snapCount {
		return false
	}

	GetSugar().Debugf("maybeTriggerSnapshot %d %d", index, rc.snapshotIndex)

	rc.snapshotting = true

	return true
}

func (rc *RaftInstance) onTriggerSnapshotOK(snap raftpb.Snapshot) {

	GetSugar().Debugf("onTriggerSnapshotOK")

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
		GetLogger().Panic("raftStorage.Compact", zap.Error(err))
	}

}

func (rc *RaftInstance) triggerSnapshot(st snapshotNotifyst) {

	var err error
	var snap raftpb.Snapshot

	//将membership写入snapshot尾部
	mbJson := rc.mb.ToJson()
	st.snapshot = buffer.AppendBytes(st.snapshot, mbJson)
	st.snapshot = buffer.AppendUint32(st.snapshot, uint32(len(mbJson)))

	//GetSugar().Infof("triggerSnapshot %v memberCount:%d snapshot Len:%d mbJson len:%d", rc.id.String(), len(rc.mb.RaftMembers()), len(st.snapshot), len(mbJson))

	snap, err = rc.raftStorage.CreateSnapshot(st.applyIdx, &rc.confState, st.snapshot)
	if err != nil {
		panic(err)
	}

	go func() {

		if err = rc.saveSnap(snap); err != nil {
			panic(err)
		}

		//删除旧快照
		rc.removeOldSnapAndWal(snap.Metadata.Term, snap.Metadata.Index-SnapshotCatchUpEntriesN)

		select {
		case rc.snapshotCh <- snap:
		case <-rc.stopc:
		}

	}()
}

func (rc *RaftInstance) saveSnap(snap raftpb.Snapshot) error {
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

func (rc *RaftInstance) loadSnapshot(haveWAL bool) (*raftpb.Snapshot, error) {
	var snapshot *raftpb.Snapshot
	var err error

	if haveWAL {
		walSnaps, err := wal.ValidSnapshotEntries(GetLogger(), rc.waldir)
		if err != nil {
			GetSugar().Errorf("ValidSnapshotEntries :%v", err)
			return nil, err
		}
		snapshot, err = rc.snapshotter.LoadNewestAvailable(walSnaps)

	} else {
		snapshot, err = rc.snapshotter.Load()
	}

	if err != nil && err != snap.ErrNoSnapshot {
		return nil, err
	} else {
		return snapshot, nil
	}
}

func (rc *RaftInstance) recoverMemberShipFromSnapshot(snap *raftpb.Snapshot) {
	dataLen := len(snap.Data)

	r := buffer.NewReader(snap.Data[dataLen-4:])
	l := int(r.GetUint32())
	r = buffer.NewReader(snap.Data[dataLen-4-l : dataLen-4])
	mbJson := r.GetBytes(l)

	if err := rc.mb.RecoverFromJson(mbJson); nil != err {
		panic(err)
	}

	//丢弃membership相关数据
	snap.Data = snap.Data[:dataLen-4-l]
}

func (rc *RaftInstance) publishSnapshot(snap raftpb.Snapshot) {
	if raft.IsEmptySnap(snap) {
		return
	}

	GetSugar().Debugf("publishing snapshot at index %d", rc.snapshotIndex)
	defer GetSugar().Debugf("finished publishing snapshot at index %d", rc.snapshotIndex)

	if snap.Metadata.Index <= rc.appliedIndex {
		GetSugar().Fatalf("snapshot index [%d] should > progress.appliedIndex [%d]", snap.Metadata.Index, rc.appliedIndex)
	}

	rc.confState = snap.Metadata.ConfState
	rc.snapshotIndex = snap.Metadata.Index
	atomic.StoreUint64(&rc.appliedIndex, snap.Metadata.Index)

	rc.transport.RemoveAllPeers()

	rc.recoverMemberShipFromSnapshot(&snap)

	for _, v := range rc.mb.Members() {
		if uint64(v.ID) != rc.id {
			rc.transport.AddPeer(types.ID(v.ID), v.PeerURLs)
		}
	}
	// trigger kvstore to load snapshot
	rc.commitC.AppendHighestPriotiryItem(snap)
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

func (rc *RaftInstance) sendSnapshot(m raftpb.Message) {
	data := make([]byte, len(m.Snapshot.Data))

	copy(data, m.Snapshot.Data)

	pr := newSnapshotReaderCloser(data)

	snapMsg := *snap.NewMessage(m, pr, int64(len(data)))

	go func() {

		now := time.Now()
		rc.transport.SendSnapshot(snapMsg)

		fields := []zap.Field{
			zap.String("from", types.ID(int(rc.ID())).String()),
			zap.String("to", types.ID(snapMsg.To).String()),
			zap.Int64("bytes", snapMsg.TotalSize),
			zap.String("size", humanize.Bytes(uint64(snapMsg.TotalSize))),
		}

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

			GetLogger().Info("sent snapshot", append(fields, zap.Duration("took", time.Since(now)))...)

		case <-rc.stopping:
			GetLogger().Warn("canceled sending merged snapshot; server stopping", fields...)
			return
		}
	}()
}
