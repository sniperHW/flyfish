package kvproxy

import (
	"github.com/sniperHW/kendynet"
	"strconv"
	"strings"
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

func newReqRounter(proxy *kvproxy) *reqRouter {

	r := &reqRouter{
		kvnodes: []*kvnode{},
	}

	kvnodes := strings.Split(GetConfig().KVNodes, ",")

	if len(kvnodes) == 0 {
		panic("len(kvnodes) == 0")
	}

	for _, v := range kvnodes {

		t := strings.Split(v, ":")

		if len(t) != 3 {
			panic("invaild nodes")
		}

		id, err := strconv.Atoi(t[0])

		if nil != err {
			panic("invaild id")
		}

		addr := t[1] + ":" + t[2]

		r.kvnodes = append(r.kvnodes, &kvnode{
			serverID:     id,
			addr:         addr,
			compressConn: openConn(proxy, id, addr, true),
			conn:         openConn(proxy, id, addr, false),
		})
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
