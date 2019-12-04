package kvproxy

import (
	"github.com/sniperHW/kendynet"
	"time"
)

type kvnode struct {
	serverID     int
	addr         string
	compressConn *Conn
	conn         *Conn
}

type reqRouter struct {
	kvnodes []*kvnode
}

func newReqRounter(kvnodes []string) *reqRouter {
	r := &reqRouter{
		kvnodes: []*kvnode{},
	}
	return r
}

func stringHash(s string) int {
	var hash uint16
	for _, c := range s {

		ch := uint16(c)

		hash = hash + ((hash) << 5) + ch + (ch << 7)
	}

	return int(hash)
}

//根据unikey将req转发到合适的kvnode
func (this *reqRouter) forward2kvnode(unikey string, sendDeadline time.Time, req *kendynet.ByteBuffer, compress bool) error {
	code := stringHash(unikey)
	node := this.kvnodes[code%len(this.kvnodes)]
	if compress {
		return node.compressConn.SendReq(sendDeadline, req)
	} else {
		return node.conn.SendReq(sendDeadline, req)
	}
}
