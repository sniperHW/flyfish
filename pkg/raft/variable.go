package raft

import (
	"time"
)

var (
	ProposalFlushInterval          = 100
	ProposalBatchCount             = 200
	ReadFlushInterval              = 50
	ReadBatchCount                 = 100
	ReadTimeout                    = time.Second * 5
	DefaultSnapshotCount    uint64 = 3000
	SnapshotCatchUpEntriesN uint64 = 3000
)

type RemoveFromCluster struct{}
type ReplayOK struct{}
type ReplaySnapshot struct{}
type RaftStopOK struct{}
type LeaderChange struct {
	Leader int
}
