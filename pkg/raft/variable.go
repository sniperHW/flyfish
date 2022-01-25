package raft

import (
	"github.com/sniperHW/flyfish/pkg/etcd/raft/raftpb"
	"time"
)

var (
	ReadTimeout                      = time.Second * 5
	SnapshotCount             uint64 = 3000
	SnapshotCatchUpEntriesN   uint64 = 1000
	MaxInFlightMsgSnap        int64  = 16
	ReleaseDelayAfterSnapshot        = 30 * time.Second
	ReadyPercent                     = 0.9
	HealthInterval                   = 5 * time.Second
	CheckQuorum               bool   = true
	MaxBatchCount             int    = 200
)

type ConfChange struct {
	CCType  raftpb.ConfChangeType
	NodeID  RaftInstanceID
	RaftUrl string
}

type ReplayOK struct{}

type RaftStopOK struct{}

type LeaderChange struct {
	Leader RaftInstanceID
}

type TransportError error
