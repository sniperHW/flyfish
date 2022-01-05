package raft

import (
	"context"
	"github.com/sniperHW/flyfish/pkg/etcd/pkg/types"
	"go.uber.org/zap"
	"time"
)

func (rc *RaftInstance) hasMultipleVotingMembers() bool {
	return len(rc.mb.VotingMemberIDs()) > 1
}

// MoveLeader transfers the leader to the given transferee.
func (rc *RaftInstance) MoveLeader(ctx context.Context, lead, transferee uint64) error {
	if !rc.mb.IsMemberExist(types.ID(transferee)) || rc.mb.Member(types.ID(transferee)).IsLearner {
		return ErrBadLeaderTransferee
	}

	now := time.Now()
	interval := time.Duration(1000) * time.Millisecond

	lg := GetLogger()
	lg.Info(
		"leadership transfer starting",
		zap.String("local-member-id", rc.id.String()),
		zap.String("current-leader-member-id", types.ID(lead).String()),
		zap.String("transferee-member-id", types.ID(transferee).String()),
	)

	rc.node.TransferLeadership(ctx, lead, transferee)
	for rc.Lead() != transferee {
		select {
		case <-ctx.Done(): // time out
			return ErrTimeoutLeaderTransfer
		case <-time.After(interval):
		}
	}

	// TODO: drain all requests, or drop all messages to the old leader
	lg.Info(
		"leadership transfer finished",
		zap.String("local-member-id", rc.id.String()),
		zap.String("old-leader-member-id", types.ID(lead).String()),
		zap.String("new-leader-member-id", types.ID(transferee).String()),
		zap.Duration("took", time.Since(now)),
	)
	return nil
}

// TransferLeadership transfers the leader to the chosen transferee.
func (rc *RaftInstance) TransferLeadership(transferee uint64) error {
	lg := GetLogger()
	if !rc.isLeader() {
		lg.Info(
			"skipped leadership transfer; local server is not leader",
			zap.String("local-member-id", rc.id.String()),
			zap.String("current-leader-member-id", rc.id.String()),
		)
		return nil
	}

	if !rc.hasMultipleVotingMembers() {
		lg.Info(
			"skipped leadership transfer for single voting member cluster",
			zap.String("local-member-id", rc.id.String()),
			zap.String("current-leader-member-id", rc.id.String()),
		)
		return nil
	}

	if rc.transport.ActiveSince(types.ID(transferee)).IsZero() {
		return ErrUnhealthy
	}

	ctx, cancel := context.WithTimeout(context.TODO(), time.Duration(time.Second))
	err := rc.MoveLeader(ctx, uint64(rc.id), uint64(transferee))
	cancel()
	return err
}
