package net

import (
	"errors"
	"github.com/sniperHW/flyfish/pkg/buffer"
)

var (
	ErrSocketClose = errors.New("socket close")
	ErrSendQueFull = errors.New("send queue full")
	ErrSendTimeout = errors.New("send timeout")
	ErrRecvTimeout = errors.New("recv timeout")
)

type Encoder interface {
	EnCode(m interface{}, b *buffer.Buffer) error
}

type InBoundProcessor interface {
	GetRecvBuff() []byte
	OnData([]byte)
	Unpack() (interface{}, error)
}
