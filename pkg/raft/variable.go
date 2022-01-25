package raft

import (
	"github.com/sniperHW/flyfish/pkg/etcd/raft/raftpb"
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
	ReadyPercent                     = 0.9
	HealthInterval                   = 5 * time.Second
	CheckQuorum               bool   = true
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
