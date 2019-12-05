package kvproxy

import (
	//"fmt"
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

func newReqRounter(proxy *kvproxy, kvnodes []string) *reqRouter {

	if len(kvnodes) == 0 {
		panic("len(kvnodes) == 0")
	}

	r := &reqRouter{
		kvnodes: []*kvnode{},
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

		r.kvnodes = append(r.kvnodes, &kvnode{
			serverID:     id,
			addr:         t[1] + ":" + t[2],
			compressConn: openConn(proxy, id, t[1]+":"+t[2], true),
			conn:         openConn(proxy, id, t[1]+":"+t[2], false),
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
