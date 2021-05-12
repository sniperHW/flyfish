package raft

import (
	"errors"
)

var (
	ERR_TIMEOUT          = errors.New("timeout")
	ERR_SERVER_STOPED    = errors.New("server stop")
	ERR_PROPOSAL_DROPPED = errors.New("proposal dropped")
	ERR_LEADER_DEMOTE    = errors.New("leader demote")
)
