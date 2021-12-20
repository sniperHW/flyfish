package raft

import (
	"errors"
)

var (
	ErrUnknownMethod              = errors.New("flyfish: unknown method")
	ErrStopped                    = errors.New("flyfish: server stopped")
	ErrCanceled                   = errors.New("flyfish: request cancelled")
	ErrTimeout                    = errors.New("flyfish: request timed out")
	ErrTimeoutDueToLeaderFail     = errors.New("flyfish: request timed out, possibly due to previous leader failure")
	ErrTimeoutDueToConnectionLost = errors.New("flyfish: request timed out, possibly due to connection lost")
	ErrTimeoutLeaderTransfer      = errors.New("flyfish: request timed out, leader transfer took too long")
	ErrLeaderChanged              = errors.New("flyfish: leader changed")
	ErrNotEnoughStartedMembers    = errors.New("flyfish: re-configuration failed due to not enough started members")
	ErrLearnerNotReady            = errors.New("flyfish: can only promote a learner member which is in sync with leader")
	ErrNoLeader                   = errors.New("flyfish: no leader")
	ErrNotLeader                  = errors.New("flyfish: not leader")
	ErrRequestTooLarge            = errors.New("flyfish: request is too large")
	ErrNoSpace                    = errors.New("flyfish: no space")
	ErrTooManyRequests            = errors.New("flyfish: too many requests")
	ErrUnhealthy                  = errors.New("flyfish: unhealthy cluster")
	ErrCorrupt                    = errors.New("flyfish: corrupt cluster")
	ErrBadLeaderTransferee        = errors.New("flyfish: bad leader transferee")
)
