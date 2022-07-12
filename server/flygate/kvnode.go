package flygate

import (
	"encoding/binary"
	"github.com/sniperHW/flyfish/errcode"
	flynet "github.com/sniperHW/flyfish/pkg/net"
	flyproto "github.com/sniperHW/flyfish/proto"
	"github.com/sniperHW/flyfish/proto/cs"
	"sync/atomic"
	"time"
)

type kvnode struct {
	id       int
	service  string
	session  *flynet.Socket
	setID    int
	waitResp containerMap
	waitSend containerList
}

func (n *kvnode) send(now time.Time, request *request) {
	binary.BigEndian.PutUint64(request.bytes[4:], uint64(request.seqno))
	binary.BigEndian.PutUint32(request.bytes[4+8:], uint32(request.store&0xFFFFFFFF))
	binary.BigEndian.PutUint32(request.bytes[18:], uint32(request.deadline.Sub(now)/time.Millisecond))
	n.session.Send(request.bytes)
}

func (g *gate) nodeSend(node *kvnode, request *request, now time.Time) {
	if request.deadline.After(now) {
		if nil != node.session {
			node.waitResp.add(request)
			node.send(now, request)
		} else {
			node.waitSend.add(request)
			if node.waitSend.l.Len() == 1 {
				go g.connectNode(node)
			}
		}
	}
}

func (g *gate) connectNode(node *kvnode) {
	if atomic.LoadInt32(&g.closed) == 0 {
		c := cs.NewConnector("tcp", node.service, flynet.OutputBufLimit{
			OutPutLimitSoft:        1024 * 1024 * 10,
			OutPutLimitSoftSeconds: 10,
			OutPutLimitHard:        1024 * 1024 * 50,
		})
		session, err := c.Dial(time.Second * 5)
		g.Lock()
		defer g.Unlock()
		if g.checkKvnode(node) {
			if nil == err {
				node.session = session
				session.SetRecvTimeout(flyproto.PingTime * 10)
				session.SetInBoundProcessor(NewKvnodeRespInboundProcessor())
				session.SetCloseCallBack(func(sess *flynet.Socket, reason error) {
					g.Lock()
					defer g.Unlock()
					node.session = nil
					for _, v := range node.waitResp.m {
						v.replyErr(errcode.New(errcode.Errcode_error, "lose connection"))
					}
				}).BeginRecv(func(s *flynet.Socket, msg interface{}) {
					g.onResponse(node, msg.([]byte))
				})

				now := time.Now()
				for v := node.waitSend.l.Front(); nil != v; v = node.waitSend.l.Front() {
					request := v.Value.(*request)
					request.remove(&node.waitSend)
					g.nodeSend(node, request, now)
				}

			} else if node.waitSend.l.Len() > 0 {
				go g.connectNode(node)
			}
		}
	}
}

func (g *gate) onResponse(node *kvnode, b []byte) {
	seqno := int64(binary.BigEndian.Uint64(b[cs.SizeLen:]))
	errCode := int16(binary.BigEndian.Uint16(b[cs.SizeLen+8+2:]))
	g.Lock()
	defer g.Unlock()
	if request := node.waitResp.m[seqno]; nil != request {
		request.remove(&node.waitResp)
		switch errCode {
		case errcode.Errcode_not_leader:
			if store := g.getStore(request.store); nil != store {
				g.onErrNotLeader(store, request)
			} else {
				g.waitSend.add(request)
			}
		case errcode.Errcode_route_info_stale, errcode.Errcode_slot_transfering:
			g.waitSend.add(request)
		default:
			request.reply(b)
		}
	}
}
