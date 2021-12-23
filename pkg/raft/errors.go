package raft

import (
	"errors"
)

var (
	ErrStopped              = errors.New("server stopped")
	ErrTimeout              = errors.New("request timed out")
	ErrLearnerNotReady      = errors.New("can only promote a learner member which is in sync with leader")
	ErrNotLeader            = errors.New("not leader")
	ErrTooManyRequests      = errors.New("too many requests")
	ErrUnhealthy            = errors.New("unhealthy cluster")
	ErrLeaderDownToFollower = errors.New("leader down to follower")
	ErrProposalDropped      = errors.New("raft proposal dropped")

	//ErrUnknownMethod              = errors.New("flyfish: unknown method")
	//ErrCanceled                   = errors.New("flyfish: request cancelled")
	//ErrTimeoutDueToLeaderFail     = errors.New("flyfish: request timed out, possibly due to previous leader failure")
	//ErrTimeoutDueToConnectionLost = errors.New("flyfish: request timed out, possibly due to connection lost")
	//ErrTimeoutLeaderTransfer      = errors.New("flyfish: request timed out, leader transfer took too long")
	//ErrLeaderChanged              = errors.New("flyfish: leader changed")
	//ErrNotEnoughStartedMembers    = errors.New("flyfish: re-configuration failed due to not enough started members")
	//ErrNoLeader                   = errors.New("flyfish: no leader")
	//ErrRequestTooLarge            = errors.New("flyfish: request is too large")
	//ErrNoSpace                    = errors.New("flyfish: no space")
	//ErrCorrupt                    = errors.New("flyfish: corrupt cluster")
	//ErrBadLeaderTransferee        = errors.New("flyfish: bad leader transferee")
)
