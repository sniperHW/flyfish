package raft

import (
	"go.etcd.io/etcd/raft/raftpb"
	"time"
)

var (
	ProposalFlushInterval            = 100
	ProposalBatchCount               = 200
	ReadFlushInterval                = 50
	ReadBatchCount                   = 100
	ReadTimeout                      = time.Second * 5
	DefaultSnapshotCount      uint64 = 3000
	SnapshotCatchUpEntriesN   uint64 = 1000
	MaxInFlightMsgSnap        int64  = 16
	ReleaseDelayAfterSnapshot        = 30 * time.Second
)

type ConfChange struct {
	CCType  raftpb.ConfChangeType
	NodeID  int
	RaftUrl string
}

type ReplayOK struct{}

type RaftStopOK struct{}

type LeaderChange struct {
	Leader int
}

type TransportError error
