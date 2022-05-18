package raft

import (
	"github.com/sniperHW/flyfish/pkg/etcd/raft/raftpb"
	"time"
)

const (
	ReadTimeout                     = time.Second * 5
	MaxInFlightMsgSnap        int64 = 16
	ReleaseDelayAfterSnapshot       = 30 * time.Second
	ReadyPercent                    = 0.95
	HealthInterval                  = 5 * time.Second
)

var (
	checkQuorum bool = true //不对外暴露，只供测试用
)

type ConfChange struct {
	CCType  raftpb.ConfChangeType
	NodeID  uint64
	RaftUrl string
}

type ReplayOK struct{}

type RaftStopOK struct{}

type LeaderChange struct {
	Leader uint64
}

type TransportError error
