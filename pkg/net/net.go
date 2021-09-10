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

func isPow2(size int) bool {
	return (size & (size - 1)) == 0
}

func sizeofPow2(size int) int {
	if isPow2(size) {
		return size
	}
	size = size - 1
	size = size | (size >> 1)
	size = size | (size >> 2)
	size = size | (size >> 4)
	size = size | (size >> 8)
	size = size | (size >> 16)
	return size + 1
}

type Encoder interface {
	EnCode(m interface{}, b *buffer.Buffer) error
}

type InBoundProcessor interface {
	GetRecvBuff() []byte
	OnData([]byte)
	Unpack() (interface{}, error)
}
