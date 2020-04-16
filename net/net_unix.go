// +build linux darwin netbsd freebsd openbsd dragonfly
package net

import (
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/sniperHW/flyfish/conf"
	"github.com/sniperHW/flyfish/net/pb"
	"github.com/sniperHW/kendynet/aio"
	"github.com/sniperHW/kendynet/socket"
	connector "github.com/sniperHW/kendynet/socket/connector/aio"
	listener "github.com/sniperHW/kendynet/socket/listener/aio"
	"net"
)

type BufferPool struct {
	pool chan []byte
}

func NewBufferPool(bufsize int) *TestBufferPool {
	size := 10
	p := &TestBufferPool{
		pool: make(chan []byte, size),
	}
	for i := 0; i < size; i++ {
		p.pool <- make([]byte, bufsize)
	}
	return p
}

func (p *BufferPool) Get() []byte {
	return <-p.pool
}

func (p *BufferPool) Put(buff []byte) {
	p.pool <- buff[:cap(buff)]
}

var aioService *aio.AioService
var buffPool *BufferPool

func NewListener(nettype, service string) (kendynet.Listener, error) {
	return listener.New(nettype, service)
}

func NewConnector(nettype string, addr string) (kendynet.Connector, error) {
	return connector.New(nettype, addr)
}
