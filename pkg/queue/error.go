package queue

import (
	"errors"
)

var (
	ErrQueueClosed = errors.New("queue closed")
	ErrQueueFull   = errors.New("queue full")
)
