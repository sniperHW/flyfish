package raft

import (
	//"github.com/sniperHW/flyfish/pkg/buffer"
	"go.etcd.io/etcd/etcdserver/api/snap"
	"go.etcd.io/etcd/raft"
	"go.etcd.io/etcd/raft/raftpb"
	"go.etcd.io/etcd/wal/walpb"
)

type snapshotNotifyst struct {
	applyIdx uint64
	snapshot []byte
}

type SnapshotNotify struct {
	notify snapshotNotifyst
	ch     chan interface{}
}

func (this *SnapshotNotify) Notify(snapshot []byte) {
	GetSugar().Infof("snapshot notify")
	this.notify.snapshot = snapshot
	this.ch <- this.notify
	GetSugar().Infof("snapshot notify ok")
}

func (rc *RaftNode) maybeTriggerSnapshot(index uint64) bool {

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

func (rc *RaftNode) onTriggerSnapshotOK(snap raftpb.Snapshot) {

	GetSugar().Infof("onTriggerSnapshotOK")

	compactIndex := uint64(1)
	if snap.Metadata.Index > SnapshotCatchUpEntriesN {
		compactIndex = snap.Metadata.Index - SnapshotCatchUpEntriesN
	}

	rc.snapshotIndex = snap.Metadata.Index

	rc.snapshotting = false

	if err := rc.raftStorage.Compact(compactIndex); err != nil {
		panic(err)
	}

	go rc.removeOldSnapAndWal(snap.Metadata.Term, compactIndex)

}

func (rc *RaftNode) triggerSnapshot(st snapshotNotifyst) {

	go func() {

		GetSugar().Infof("triggerSnapshot")

		var err error
		var snap raftpb.Snapshot

		snap, err = rc.raftStorage.CreateSnapshot(st.applyIdx, &rc.confState, st.snapshot)
		if err != nil {
			panic(err)
		}

		if err = rc.saveSnap(snap); err != nil {
			panic(err)
		}

		rc.snapshotCh <- snap

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
	rc.commitC.AppendHighestPriotiryItem(ReplaySnapshot{})

	rc.confState = snapshotToSave.Metadata.ConfState
	rc.snapshotIndex = snapshotToSave.Metadata.Index
	rc.appliedIndex = snapshotToSave.Metadata.Index
}
