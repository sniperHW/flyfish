package membership

import (
	"errors"
)

var (
	ErrIDRemoved        = errors.New("membership: ID removed")
	ErrIDExists         = errors.New("membership: ID exists")
	ErrIDNotFound       = errors.New("membership: ID not found")
	ErrPeerURLexists    = errors.New("membership: peerURL exists")
	ErrMemberNotLearner = errors.New("membership: can only promote a learner member")
	ErrTooManyLearners  = errors.New("membership: too many learner members in cluster")
)
